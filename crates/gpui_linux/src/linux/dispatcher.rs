use calloop::PostAction;
#[cfg(not(target_os = "illumos"))]
use calloop::channel::Sender;
#[cfg(not(target_os = "illumos"))]
use calloop::{EventLoop, channel, timer::TimeoutAction};
#[cfg(not(target_os = "illumos"))]
use gpui_util::ResultExt;

#[cfg(target_os = "illumos")]
use super::illumos_ping as ping;
#[cfg(not(target_os = "illumos"))]
use calloop::ping;

#[cfg(not(target_os = "illumos"))]
use std::mem::MaybeUninit;
use std::{thread, time::Duration};

#[cfg(target_os = "illumos")]
use std::{cmp::Ordering, collections::BinaryHeap, sync::mpsc, time::Instant};

use gpui::{
    PlatformDispatcher, Priority, PriorityQueueReceiver, PriorityQueueSender, RunnableVariant,
    profiler,
};

struct TimerAfter<T = RunnableVariant> {
    duration: Duration,
    runnable: T,
}

#[cfg(target_os = "illumos")]
type TimerSender = mpsc::Sender<TimerAfter>;
#[cfg(not(target_os = "illumos"))]
type TimerSender = Sender<TimerAfter>;

#[cfg(target_os = "illumos")]
struct ScheduledTimer<T> {
    deadline: Instant,
    sequence: u64,
    runnable: T,
}

#[cfg(target_os = "illumos")]
impl<T> PartialEq for ScheduledTimer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

#[cfg(target_os = "illumos")]
impl<T> Eq for ScheduledTimer<T> {}

#[cfg(target_os = "illumos")]
impl<T> PartialOrd for ScheduledTimer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(target_os = "illumos")]
impl<T> Ord for ScheduledTimer<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[cfg(target_os = "illumos")]
fn run_illumos_timer_queue<T>(
    receiver: mpsc::Receiver<TimerAfter<T>>,
    mut fire: impl FnMut(T),
) -> Vec<T> {
    let mut timers = BinaryHeap::<ScheduledTimer<T>>::new();
    let mut sequence = 0u64;

    loop {
        let now = Instant::now();
        while timers.peek().is_some_and(|timer| timer.deadline <= now) {
            let timer = timers.pop().expect("timer heap was not empty");
            fire(timer.runnable);
        }

        let timer = if let Some(next) = timers.peek() {
            match receiver.recv_timeout(next.deadline.saturating_duration_since(Instant::now())) {
                Ok(timer) => timer,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match receiver.recv() {
                Ok(timer) => timer,
                Err(_) => break,
            }
        };

        timers.push(ScheduledTimer {
            deadline: Instant::now() + timer.duration,
            sequence,
            runnable: timer.runnable,
        });
        sequence = sequence.wrapping_add(1);
    }

    timers.into_iter().map(|timer| timer.runnable).collect()
}

pub(crate) struct LinuxDispatcher {
    main_sender: PriorityQueueCalloopSender<RunnableVariant>,
    timer_sender: TimerSender,
    background_sender: PriorityQueueSender<RunnableVariant>,
    _background_threads: Vec<thread::JoinHandle<()>>,
    main_thread_id: thread::ThreadId,
}

const MIN_THREADS: usize = 2;

impl LinuxDispatcher {
    pub fn new(main_sender: PriorityQueueCalloopSender<RunnableVariant>) -> Self {
        let (background_sender, background_receiver) = PriorityQueueReceiver::new();
        let thread_count =
            std::thread::available_parallelism().map_or(MIN_THREADS, |i| i.get().max(MIN_THREADS));

        let mut background_threads = (0..thread_count)
            .map(|i| {
                let receiver: PriorityQueueReceiver<RunnableVariant> = background_receiver.clone();
                std::thread::Builder::new()
                    .name(format!("Worker-{i}"))
                    .spawn(move || {
                        for runnable in receiver.iter() {
                            let location = runnable.metadata().location;
                            let spawned = runnable.metadata().spawned;
                            profiler::update_running_task(spawned, location);
                            runnable.run();
                            profiler::save_task_timing();
                        }
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();

        #[cfg(not(target_os = "illumos"))]
        let (timer_sender, timer_channel) = calloop::channel::channel::<TimerAfter>();
        #[cfg(target_os = "illumos")]
        let (timer_sender, timer_channel) = mpsc::channel::<TimerAfter>();

        #[cfg(target_os = "illumos")]
        let timer_thread = std::thread::Builder::new()
            .name("Timer".to_owned())
            .spawn(move || {
                let pending =
                    run_illumos_timer_queue(timer_channel, |runnable: RunnableVariant| {
                        let location = runnable.metadata().location;
                        let spawned = runnable.metadata().spawned;
                        profiler::update_running_task(spawned, location);
                        runnable.run();
                        profiler::save_task_timing();
                    });

                // Dropping a scheduled runnable cancels its task and makes the next poll of any
                // awaiter panic. Keep pending tasks pending while the process shuts down.
                for runnable in pending {
                    std::mem::forget(runnable);
                }
            })
            .unwrap();

        #[cfg(not(target_os = "illumos"))]
        let timer_thread = std::thread::Builder::new()
            .name("Timer".to_owned())
            .spawn(move || {
                let mut event_loop: EventLoop<()> =
                    EventLoop::try_new().expect("Failed to initialize timer loop!");

                let handle = event_loop.handle();
                let timer_handle = event_loop.handle();
                handle
                    .insert_source(timer_channel, move |e, _, _| {
                        if let channel::Event::Msg(timer) = e {
                            let mut runnable = Some(timer.runnable);
                            timer_handle
                                .insert_source(
                                    calloop::timer::Timer::from_duration(timer.duration),
                                    move |_, _, _| {
                                        if let Some(runnable) = runnable.take() {
                                            let location = runnable.metadata().location;
                                            let spawned = runnable.metadata().spawned;
                                            profiler::update_running_task(spawned, location);
                                            runnable.run();
                                            profiler::save_task_timing();
                                        }
                                        TimeoutAction::Drop
                                    },
                                )
                                .expect("Failed to start timer");
                        }
                    })
                    .expect("Failed to start timer thread");

                event_loop.run(None, &mut (), |_| {}).log_err();
            })
            .unwrap();

        background_threads.push(timer_thread);

        Self {
            main_sender,
            timer_sender,
            background_sender,
            _background_threads: background_threads,
            main_thread_id: thread::current().id(),
        }
    }
}

impl PlatformDispatcher for LinuxDispatcher {
    fn is_main_thread(&self) -> bool {
        thread::current().id() == self.main_thread_id
    }

    fn dispatch(&self, runnable: RunnableVariant, priority: Priority) {
        self.background_sender
            .send(priority, runnable)
            .unwrap_or_else(|_| panic!("blocking sender returned without value"));
    }

    fn dispatch_on_main_thread(&self, runnable: RunnableVariant, priority: Priority) {
        self.main_sender
            .send(priority, runnable)
            .unwrap_or_else(|runnable| {
                // NOTE: Runnable may wrap a Future that is !Send.
                //
                // This is usually safe because we only poll it on the main thread.
                // However if the send fails, we know that:
                // 1. main_receiver has been dropped (which implies the app is shutting down)
                // 2. we are on a background thread.
                // It is not safe to drop something !Send on the wrong thread, and
                // the app will exit soon anyway, so we must forget the runnable.
                std::mem::forget(runnable);
            });
    }

    fn dispatch_after(&self, duration: Duration, runnable: RunnableVariant) {
        let result = self.timer_sender.send(TimerAfter { duration, runnable });

        if let Err(err) = result {
            // The timer thread has shut down. Dropping a scheduled runnable cancels its task
            // and makes the next poll of any awaiter panic. Leaking leaves the task pending,
            // which is acceptable during shutdown.
            std::mem::forget(err);
        }
    }

    fn spawn_realtime(&self, f: Box<dyn FnOnce() + Send>) {
        std::thread::spawn(move || {
            // libc does not expose pthread_setschedparam on illumos, so the
            // thread runs at default priority there.
            #[cfg(not(target_os = "illumos"))]
            {
                // SAFETY: always safe to call
                let thread_id = unsafe { libc::pthread_self() };

                let policy = libc::SCHED_FIFO;
                let sched_priority = 65;

                // SAFETY: all sched_param members are valid when initialized to zero.
                let mut sched_param =
                    unsafe { MaybeUninit::<libc::sched_param>::zeroed().assume_init() };
                sched_param.sched_priority = sched_priority;
                // SAFETY: sched_param is a valid initialized structure
                let result =
                    unsafe { libc::pthread_setschedparam(thread_id, policy, &sched_param) };
                if result != 0 {
                    log::warn!("failed to set realtime thread priority");
                }
            }

            f();
        });
    }
}

pub struct PriorityQueueCalloopSender<T> {
    sender: PriorityQueueSender<T>,
    ping: ping::Ping,
}

impl<T> PriorityQueueCalloopSender<T> {
    fn new(tx: PriorityQueueSender<T>, ping: ping::Ping) -> Self {
        Self { sender: tx, ping }
    }

    fn send(&self, priority: Priority, item: T) -> Result<(), gpui::queue::SendError<T>> {
        let res = self.sender.send(priority, item);
        if res.is_ok() {
            self.ping.ping();
        }
        res
    }
}

impl<T> Drop for PriorityQueueCalloopSender<T> {
    fn drop(&mut self) {
        self.ping.ping();
    }
}

pub struct PriorityQueueCalloopReceiver<T> {
    receiver: PriorityQueueReceiver<T>,
    source: ping::PingSource,
    ping: ping::Ping,
}

impl<T> PriorityQueueCalloopReceiver<T> {
    pub fn new() -> (PriorityQueueCalloopSender<T>, Self) {
        let (ping, source) = ping::make_ping().expect("Failed to create a Ping.");

        let (tx, rx) = PriorityQueueReceiver::new();

        (
            PriorityQueueCalloopSender::new(tx, ping.clone()),
            Self {
                receiver: rx,
                source,
                ping,
            },
        )
    }
}

use calloop::channel::Event;

#[derive(Debug)]
pub struct ChannelError(ping::PingError);

impl std::fmt::Display for ChannelError {
    #[cfg_attr(feature = "nightly_coverage", coverage(off))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ChannelError {
    #[cfg_attr(feature = "nightly_coverage", coverage(off))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl<T> calloop::EventSource for PriorityQueueCalloopReceiver<T> {
    type Event = Event<T>;
    type Metadata = ();
    type Ret = ();
    type Error = ChannelError;

    fn process_events<F>(
        &mut self,
        readiness: calloop::Readiness,
        token: calloop::Token,
        mut callback: F,
    ) -> Result<calloop::PostAction, Self::Error>
    where
        F: FnMut(Self::Event, &mut Self::Metadata) -> Self::Ret,
    {
        let mut clear_readiness = false;
        let mut disconnected = false;

        let action = self
            .source
            .process_events(readiness, token, |(), &mut ()| {
                let mut is_empty = true;

                let receiver = self.receiver.clone();
                for runnable in receiver.try_iter() {
                    match runnable {
                        Ok(r) => {
                            callback(Event::Msg(r), &mut ());
                            is_empty = false;
                        }
                        Err(_) => {
                            disconnected = true;
                        }
                    }
                }

                if disconnected {
                    callback(Event::Closed, &mut ());
                }

                if is_empty {
                    clear_readiness = true;
                }
            })
            .map_err(ChannelError)?;

        if disconnected {
            Ok(PostAction::Remove)
        } else if clear_readiness {
            Ok(action)
        } else {
            // Re-notify the ping source so we can try again.
            self.ping.ping();
            Ok(action)
        }
    }

    fn register(
        &mut self,
        poll: &mut calloop::Poll,
        token_factory: &mut calloop::TokenFactory,
    ) -> calloop::Result<()> {
        self.source.register(poll, token_factory)
    }

    fn reregister(
        &mut self,
        poll: &mut calloop::Poll,
        token_factory: &mut calloop::TokenFactory,
    ) -> calloop::Result<()> {
        self.source.reregister(poll, token_factory)
    }

    fn unregister(&mut self, poll: &mut calloop::Poll) -> calloop::Result<()> {
        self.source.unregister(poll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "illumos")]
    #[test]
    fn illumos_timer_queue_survives_burst_and_idle() {
        let (timer_tx, timer_rx) = mpsc::channel();
        let (fired_tx, fired_rx) = mpsc::channel();
        let timer_thread = thread::spawn(move || {
            run_illumos_timer_queue(timer_rx, move |value| {
                fired_tx.send(value).unwrap();
            })
        });

        const TIMER_COUNT: usize = 512;
        for value in 0..TIMER_COUNT {
            timer_tx
                .send(TimerAfter {
                    duration: Duration::from_millis((value % 4) as u64),
                    runnable: value,
                })
                .unwrap();
        }

        let mut seen = vec![false; TIMER_COUNT];
        for _ in 0..TIMER_COUNT {
            let value = fired_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(!seen[value], "timer {value} fired twice");
            seen[value] = true;
        }
        assert!(seen.into_iter().all(|fired| fired));

        thread::sleep(Duration::from_millis(50));
        timer_tx
            .send(TimerAfter {
                duration: Duration::from_millis(10),
                runnable: usize::MAX,
            })
            .unwrap();
        assert_eq!(
            fired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            usize::MAX
        );

        drop(timer_tx);
        assert!(timer_thread.join().unwrap().is_empty());
    }

    #[test]
    fn calloop_works() {
        let mut event_loop = calloop::EventLoop::try_new().unwrap();
        let handle = event_loop.handle();

        let (tx, rx) = PriorityQueueCalloopReceiver::new();

        struct Data {
            got_msg: bool,
            got_closed: bool,
        }

        let mut data = Data {
            got_msg: false,
            got_closed: false,
        };

        let _channel_token = handle
            .insert_source(rx, move |evt, &mut (), data: &mut Data| match evt {
                Event::Msg(()) => {
                    data.got_msg = true;
                }

                Event::Closed => {
                    data.got_closed = true;
                }
            })
            .unwrap();

        // nothing is sent, nothing is received
        event_loop
            .dispatch(Some(::std::time::Duration::ZERO), &mut data)
            .unwrap();

        assert!(!data.got_msg);
        assert!(!data.got_closed);
        // a message is send

        tx.send(Priority::Medium, ()).unwrap();
        event_loop
            .dispatch(Some(::std::time::Duration::ZERO), &mut data)
            .unwrap();

        assert!(data.got_msg);
        assert!(!data.got_closed);

        // the sender is dropped
        drop(tx);
        event_loop
            .dispatch(Some(::std::time::Duration::ZERO), &mut data)
            .unwrap();

        assert!(data.got_msg);
        assert!(data.got_closed);
    }
}
