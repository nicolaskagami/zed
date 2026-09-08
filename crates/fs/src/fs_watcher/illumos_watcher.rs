use super::{ILLUMOS_FEN_EVENT, WatchBackend};
use notify::{
    Event, EventKind, RecursiveMode,
    event::{ModifyKind, RemoveKind, RenameMode},
};
use std::{
    collections::HashMap,
    ffi::CString,
    fs::Metadata,
    io,
    os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _},
    path::{Path, PathBuf},
    ptr,
    sync::mpsc,
    thread,
};

const FILE_MODIFIED: libc::c_int = 0x0000_0002;
const FILE_ATTRIB: libc::c_int = 0x0000_0004;
const FILE_DELETE: libc::c_int = 0x0000_0010;
const FILE_RENAME_TO: libc::c_int = 0x0000_0020;
const FILE_RENAME_FROM: libc::c_int = 0x0000_0040;
const FILE_NOFOLLOW: libc::c_int = 0x1000_0000;
const UNMOUNTED: libc::c_int = 0x2000_0000;
const MOUNTEDOVER: libc::c_int = 0x4000_0000;
const WATCH_EVENTS: libc::c_int = FILE_MODIFIED | FILE_ATTRIB | FILE_NOFOLLOW;
const TERMINAL_EVENTS: libc::c_int =
    FILE_DELETE | FILE_RENAME_TO | FILE_RENAME_FROM | UNMOUNTED | MOUNTEDOVER;

#[repr(C)]
struct FileObject {
    atime: libc::timespec,
    mtime: libc::timespec,
    ctime: libc::timespec,
    pad: [libc::uintptr_t; 3],
    name: *mut libc::c_char,
}

struct WatchEntry {
    name: CString,
    object: FileObject,
}

impl WatchEntry {
    fn new(path: PathBuf) -> notify::Result<Self> {
        let name = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| notify::Error::generic("path contains a NUL byte"))?;
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(notify::Error::io_watch)
            .map_err(|error| error.add_path(path.clone()))?;
        let mut entry = Self {
            name,
            object: FileObject {
                atime: timestamp(metadata.atime(), metadata.atime_nsec()),
                mtime: timestamp(metadata.mtime(), metadata.mtime_nsec()),
                ctime: timestamp(metadata.ctime(), metadata.ctime_nsec()),
                pad: [0; 3],
                name: ptr::null_mut(),
            },
        };
        entry.object.name = entry.name.as_ptr().cast_mut();
        Ok(entry)
    }

    fn refresh(&mut self, metadata: &Metadata) {
        self.object.atime = timestamp(metadata.atime(), metadata.atime_nsec());
        self.object.mtime = timestamp(metadata.mtime(), metadata.mtime_nsec());
        self.object.ctime = timestamp(metadata.ctime(), metadata.ctime_nsec());
    }

    fn object_address(&mut self) -> libc::uintptr_t {
        (&mut self.object as *mut FileObject) as libc::uintptr_t
    }
}

fn timestamp(seconds: i64, nanoseconds: i64) -> libc::timespec {
    libc::timespec {
        tv_sec: seconds,
        tv_nsec: nanoseconds,
    }
}

enum Command {
    Watch(PathBuf, RecursiveMode, mpsc::Sender<notify::Result<()>>),
    Unwatch(PathBuf, mpsc::Sender<notify::Result<()>>),
    Shutdown,
}

pub(super) struct IllumosWatcher {
    port: libc::c_int,
    commands: mpsc::Sender<Command>,
    thread: Option<thread::JoinHandle<()>>,
}

impl IllumosWatcher {
    pub(super) fn new(
        handler: impl FnMut(notify::Result<Event>) + Send + 'static,
    ) -> notify::Result<Self> {
        // SAFETY: port_create has no preconditions.
        let port = unsafe { libc::port_create() };
        if port < 0 {
            return Err(notify::Error::io(io::Error::last_os_error()));
        }

        let (commands, command_rx) = mpsc::channel();
        let thread = match thread::Builder::new()
            .name("illumos file events".to_owned())
            .spawn(move || run(port, command_rx, handler))
        {
            Ok(thread) => thread,
            Err(error) => {
                // SAFETY: port was returned by port_create and is still owned here.
                unsafe { libc::close(port) };
                return Err(notify::Error::io(error));
            }
        };

        Ok(Self {
            port,
            commands,
            thread: Some(thread),
        })
    }

    fn command(
        &self,
        command: Command,
        reply: mpsc::Receiver<notify::Result<()>>,
    ) -> notify::Result<()> {
        self.commands
            .send(command)
            .map_err(|_| notify::Error::generic("Illumos file-event thread stopped"))?;
        self.wake()?;
        reply
            .recv()
            .map_err(|_| notify::Error::generic("Illumos file-event thread stopped"))?
    }

    fn wake(&self) -> notify::Result<()> {
        // SAFETY: port remains open for the lifetime of this watcher. A user event
        // wakes the worker so it can drain its command channel.
        if unsafe { libc::port_send(self.port, 0, ptr::null_mut()) } == 0 {
            Ok(())
        } else {
            Err(notify::Error::io(io::Error::last_os_error()))
        }
    }
}

impl WatchBackend for IllumosWatcher {
    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command(Command::Watch(path.to_path_buf(), mode, reply_tx), reply_rx)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command(Command::Unwatch(path.to_path_buf(), reply_tx), reply_rx)
    }
}

impl Drop for IllumosWatcher {
    fn drop(&mut self) {
        if self.commands.send(Command::Shutdown).is_ok() {
            self.wake().ok();
        }
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
    }
}

fn run(
    port: libc::c_int,
    commands: mpsc::Receiver<Command>,
    mut handler: impl FnMut(notify::Result<Event>),
) {
    let mut entries = HashMap::<PathBuf, Box<WatchEntry>>::new();
    let mut object_paths = HashMap::<libc::uintptr_t, PathBuf>::new();

    loop {
        let mut event = std::mem::MaybeUninit::<libc::port_event>::zeroed();
        // SAFETY: port is valid, event points to writable storage, and a null
        // timeout requests an indefinite wait.
        if unsafe { libc::port_get(port, event.as_mut_ptr(), ptr::null_mut()) } != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            handler(Err(notify::Error::io(error)));
            break;
        }
        // SAFETY: a successful port_get initialized event.
        let event = unsafe { event.assume_init() };

        if event.portev_source as libc::c_int == libc::PORT_SOURCE_USER {
            if !handle_commands(port, &commands, &mut entries, &mut object_paths) {
                break;
            }
        } else if event.portev_source as libc::c_int == libc::PORT_SOURCE_FILE {
            handle_file_event(port, event, &mut entries, &mut object_paths, &mut handler);
        }
    }

    // SAFETY: the worker owns port and no other thread closes it.
    unsafe { libc::close(port) };
}

fn handle_commands(
    port: libc::c_int,
    commands: &mpsc::Receiver<Command>,
    entries: &mut HashMap<PathBuf, Box<WatchEntry>>,
    object_paths: &mut HashMap<libc::uintptr_t, PathBuf>,
) -> bool {
    while let Ok(command) = commands.try_recv() {
        match command {
            Command::Watch(path, mode, reply) => {
                let result = if mode == RecursiveMode::Recursive {
                    Err(notify::Error::generic(
                        "recursive Illumos file-event watches are unsupported",
                    ))
                } else if entries.contains_key(&path) {
                    Ok(())
                } else {
                    add_watch(port, path, entries, object_paths)
                };
                reply.send(result).ok();
            }
            Command::Unwatch(path, reply) => {
                reply
                    .send(remove_watch(port, &path, entries, object_paths))
                    .ok();
            }
            Command::Shutdown => return false,
        }
    }
    true
}

fn add_watch(
    port: libc::c_int,
    path: PathBuf,
    entries: &mut HashMap<PathBuf, Box<WatchEntry>>,
    object_paths: &mut HashMap<libc::uintptr_t, PathBuf>,
) -> notify::Result<()> {
    let mut entry = Box::new(WatchEntry::new(path.clone())?);
    let object = entry.object_address();
    associate(port, object).map_err(|error| error.add_path(path.clone()))?;
    object_paths.insert(object, path.clone());
    entries.insert(path, entry);
    Ok(())
}

fn remove_watch(
    port: libc::c_int,
    path: &Path,
    entries: &mut HashMap<PathBuf, Box<WatchEntry>>,
    object_paths: &mut HashMap<libc::uintptr_t, PathBuf>,
) -> notify::Result<()> {
    let Some(mut entry) = entries.remove(path) else {
        return Err(notify::Error::watch_not_found().add_path(path.to_path_buf()));
    };
    let object = entry.object_address();
    object_paths.remove(&object);

    // An event already waiting in the port has removed the association, so
    // ENOENT is equivalent to a successful unwatch.
    // SAFETY: object points to entry, which remains alive through this call.
    if unsafe { libc::port_dissociate(port, libc::PORT_SOURCE_FILE, object) } == 0
        || io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT)
    {
        Ok(())
    } else {
        Err(notify::Error::io(io::Error::last_os_error()).add_path(path.to_path_buf()))
    }
}

fn associate(port: libc::c_int, object: libc::uintptr_t) -> notify::Result<()> {
    // SAFETY: object addresses a live FileObject and remains stable while it is
    // associated with the port.
    if unsafe {
        libc::port_associate(
            port,
            libc::PORT_SOURCE_FILE,
            object,
            WATCH_EVENTS,
            ptr::null_mut(),
        )
    } == 0
    {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EAGAIN) {
        Err(notify::Error::new(notify::ErrorKind::MaxFilesWatch))
    } else {
        Err(notify::Error::io_watch(error))
    }
}

fn handle_file_event(
    port: libc::c_int,
    event: libc::port_event,
    entries: &mut HashMap<PathBuf, Box<WatchEntry>>,
    object_paths: &mut HashMap<libc::uintptr_t, PathBuf>,
    handler: &mut impl FnMut(notify::Result<Event>),
) {
    let object = event.portev_object;
    let Some(path) = object_paths.get(&object).cloned() else {
        return;
    };

    let terminal = event.portev_events & TERMINAL_EVENTS != 0;
    let kind = if event.portev_events & FILE_DELETE != 0 {
        EventKind::Remove(RemoveKind::Any)
    } else if event.portev_events & (FILE_RENAME_TO | FILE_RENAME_FROM) != 0 {
        EventKind::Modify(ModifyKind::Name(RenameMode::Any))
    } else {
        // A directory FEN event does not identify the changed child. Reporting
        // the watched directory makes Zed scan just that directory.
        EventKind::Modify(ModifyKind::Any)
    };
    handler(Ok(Event::new(kind)
        .add_path(path.clone())
        .set_info(ILLUMOS_FEN_EVENT)));

    if terminal {
        entries.remove(&path);
        object_paths.remove(&object);
        return;
    }

    let Some(entry) = entries.get_mut(&path) else {
        return;
    };
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            entry.refresh(&metadata);
            if let Err(error) = associate(port, object) {
                handler(Err(error.add_path(path)));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            entries.remove(&path);
            object_paths.remove(&object);
        }
        Err(error) => handler(Err(notify::Error::io(error).add_path(path))),
    }
}
