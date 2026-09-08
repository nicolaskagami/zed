use calloop::{
    EventSource, Interest, Mode, Poll, PostAction, Readiness, Token, TokenFactory, generic::Generic,
};
use std::{
    fmt, io,
    io::{Read as _, Write as _},
    os::unix::net::UnixStream,
    sync::Arc,
};

pub fn make_ping() -> io::Result<(Ping, PingSource)> {
    let (read, write) = UnixStream::pair()?;
    read.set_nonblocking(true)?;
    write.set_nonblocking(true)?;

    Ok((
        Ping {
            socket: Arc::new(write),
        },
        PingSource {
            socket: Generic::new(read, Interest::READ, Mode::OneShot),
        },
    ))
}

#[derive(Clone, Debug)]
pub struct Ping {
    socket: Arc<UnixStream>,
}

impl Ping {
    pub fn ping(&self) {
        let mut socket = &*self.socket;
        match socket.write(&[0]) {
            Ok(_) => {}
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => log::warn!("failed to write an Illumos event-loop ping: {err}"),
        }
    }
}

#[derive(Debug)]
pub struct PingSource {
    socket: Generic<UnixStream>,
}

impl EventSource for PingSource {
    type Event = ();
    type Metadata = ();
    type Ret = ();
    type Error = PingError;

    fn process_events<F>(
        &mut self,
        readiness: Readiness,
        token: Token,
        mut callback: F,
    ) -> Result<PostAction, Self::Error>
    where
        F: FnMut(Self::Event, &mut Self::Metadata),
    {
        self.socket
            .process_events(readiness, token, |_, socket| {
                let mut socket = &**socket;
                let mut buffer = [0; 64];
                let mut received_ping = false;

                loop {
                    match socket.read(&mut buffer) {
                        Ok(0) => return Ok(PostAction::Remove),
                        Ok(_) => received_ping = true,
                        Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => return Err(err),
                    }
                }

                if received_ping {
                    callback((), &mut ());
                }

                // Illumos event ports are one-shot. Re-arm only after the
                // socket has been drained, so readiness cannot feed back into
                // another notification for the same ping.
                Ok(PostAction::Reregister)
            })
            .map_err(PingError)
    }

    fn register(
        &mut self,
        poll: &mut Poll,
        token_factory: &mut TokenFactory,
    ) -> calloop::Result<()> {
        self.socket.register(poll, token_factory)
    }

    fn reregister(
        &mut self,
        poll: &mut Poll,
        token_factory: &mut TokenFactory,
    ) -> calloop::Result<()> {
        self.socket.reregister(poll, token_factory)
    }

    fn unregister(&mut self, poll: &mut Poll) -> calloop::Result<()> {
        self.socket.unregister(poll)
    }
}

#[derive(Debug)]
pub struct PingError(io::Error);

impl fmt::Display for PingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for PingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
