use crate::util::lossy_channel::{lossy_channel, LossyReceiver, LossySender};
use crate::util::FAILED_TO_LOCK;
use crossbeam::channel::{self, bounded, Receiver, RecvError, Sender, TryRecvError, TrySendError};
use crossbeam::select;
use std::any::Any;
use std::io::Write;
use std::marker::PhantomData;
use std::net::TcpStream;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

pub fn fork<T: Write + Send + 'static>(
    queue_size: usize,
    topic: &str,
) -> (TargetList<T>, Arc<DataStream>) {
    // Zero-sized bounded channels are a special case in crossbeam, because
    // - they don't do busy waiting,
    // - send/recv operations block until the other is also called.
    //
    // This is good because we don't want to waste CPU on busy-waiting for channels that might be
    // low-traffic:
    // - {tx,rx}_streams is used only when our Publisher gains a new subscriber
    // - {tx,rx}_subscriber_gone is similarly only used when one of our subscribers quits
    // - {tx,rx}_queue_size is only used when changing the queue size (when does this even happen?)
    // - {tx,rx}_data is used when sending messages, which has varying amounts of traffic depending
    //   on the topic, but that shouldn't warrant wasting CPU on busy-waiting in almost all cases.
    let (tx_streams, rx_streams) = bounded::<SubscriberInfo<T>>(0);
    let (tx_data, rx_data) = bounded::<Arc<Vec<u8>>>(0);
    let (tx_queue_size, rx_queue_size) = bounded::<SetQueueSize>(0);
    let (tx_subscriber_gone, rx_subscriber_gone) = bounded::<u64>(0);

    let target_names = Arc::new(Mutex::new(TargetNames::default()));

    log::debug!(
        "fork ({}): {:?}",
        &topic,
        thread_priority::get_current_thread_priority()
    );

    let join_handle = Some(thread::Builder::new()
        .name(format!("F{}", &topic))
        .spawn({
            let target_names = Arc::clone(&target_names);
            let topic = topic.to_owned();
            move || {
                log::debug!("thread that listens for new connections ({}): {:?}", &topic, thread_priority::get_current_thread_priority()); 
                let mut senders = Vec::new();
                let mut queue_size = queue_size;
                let mut subscriber_id = 0u64;
                loop {
                    select! {
                        recv(rx_streams) -> new_conn_res => {
                            match new_conn_res {
                                Ok(new_connection) => {
                                    subscriber_id += 1;
                                    let mut target_names = target_names
                                        .lock()
                                        .unwrap();
                                    let sender = SenderSingleSubscriber::new(
                                        subscriber_id,
                                        new_connection.stream,
                                        new_connection.caller_id.clone(),
                                        queue_size,
                                        tx_subscriber_gone.clone()
                                    );
                                    senders.push(sender);
                                    target_names
                                        .targets
                                        .push(new_connection.caller_id);
                                },
                                Err(crossbeam::channel::RecvError) => {
                                    break
                                }
                            }
                        },
                        recv(rx_subscriber_gone) -> subscriber_gone_res => {
                            match subscriber_gone_res {
                                Ok(subscriber_id) => {
                                    let mut target_names = target_names.lock().unwrap();
                                    if let Some(idx) = senders.iter().position(|s| s.id == subscriber_id) {
                                        senders[idx].is_live.store(false, std::sync::atomic::Ordering::SeqCst);
                                        log::debug!("=> senders.remove(idx = {}, subscriber_id = {})", idx, subscriber_id);
                                        senders.remove(idx);
                                        log::debug!("<= senders.remove(idx = {}, subscriber_id = {})", idx, subscriber_id);
                                        target_names.targets.remove(idx);
                                    }
                                },
                                Err(e) => {
                                    log::debug!("error on rx_subscriber_gone: {:?}", e);
                                }
                            }
                        },
                        recv(rx_data) -> data => {
                            let Ok(data) = data else {
                                log::debug!("DataStream rx_data got error, ending");
                                break;
                            };
                            let mut disconnected_clients = Vec::<usize>::new();
                            for (idx, sender) in senders.iter().enumerate() {
                                match sender.try_send(Arc::clone(&data)) {
                                    Ok(()) => {},
                                    Err(TrySendError::Full(_)) => {
                                        panic!("LossyChannel returned TrySendError::Full, this should not happen.");
                                    }
                                    Err(TrySendError::Disconnected(_)) => {
                                        log::info!("client '{}' disconnected", &sender.name);
                                        disconnected_clients.push(idx);
                                    }
                                }
                            };

                            if !disconnected_clients.is_empty() {
                                let mut target_names = target_names.lock().unwrap();
                                for idx in disconnected_clients {
                                    senders[idx].is_live.store(false, std::sync::atomic::Ordering::SeqCst);
                                    target_names.targets.remove(idx);
                                    let subsriber_id = senders[idx].id;
                                    log::debug!("=> senders.remove(idx = {}, subscriber_id = {})", idx, subscriber_id);
                                    senders.remove(idx);
                                    log::debug!("<= senders.remove(idx = {}, subscriber_id = {})", idx, subscriber_id);
                                }
                            }
                        },
                        recv(rx_queue_size) -> set_queue_size => {
                            let Ok(set_queue_size) = set_queue_size else {
                                // channel closed, quit loop
                                break;
                            };
                            if set_queue_size.max {
                                queue_size = queue_size.max(set_queue_size.queue_size);
                                for sender in senders.iter() {
                                    sender.set_queue_size_max(queue_size);
                                }
                            } else {
                                queue_size = set_queue_size.queue_size;
                                for sender in senders.iter() {
                                    sender.set_queue_size(queue_size);
                                }
                            }
                        }
                    }
                }
                // We're not going to poll this anymore, so don't block the SenderSingleSubscriber
                // instances trying to report their own exit here
                drop(rx_subscriber_gone);
                log::debug!("DataStream thread ending, dropping senders... ({}→ {:?})", &topic, target_names.lock().unwrap().names());
                drop(senders);
                log::debug!("...senders dropped ({})", &topic);
            }
        })
        .expect("failed to spawn thread"));

    (
        TargetList(tx_streams),
        Arc::new(DataStream {
            target_names,
            join_handle,
            tx_data: Some(tx_data),
            tx_queue_size,
            topic_name: topic.to_owned(),
        }),
    )
}

pub type ForkResult = Result<(), ()>;

pub struct TargetList<T: Write + Send + 'static>(Sender<SubscriberInfo<T>>);

impl<T: Write + Send + 'static> TargetList<T> {
    pub fn add(&self, caller_id: String, stream: T) -> ForkResult {
        self.0
            .send(SubscriberInfo { caller_id, stream })
            .or(Err(()))
    }
}

/// Wrapper around TcpStream (or similar `T: Write + Send + 'static`) with a `LossyChannel` inbetween
struct SenderSingleSubscriber<T: Write + Send + 'static> {
    id: u64,
    tx: LossySender<Arc<Vec<u8>>>,
    join_handle: Option<JoinHandle<()>>,
    name: String,
    tx_subscriber_gone: Sender<u64>,
    // If we're still live on drop, notify the DataStream instance.
    is_live: Arc<AtomicBool>,
    _transport_type: PhantomData<T>,
}

impl<T: Write + Send + 'static> SenderSingleSubscriber<T> {
    fn new(
        id: u64,
        transport: T,
        name: String,
        queue_size: usize,
        tx_subscriber_gone: Sender<u64>,
    ) -> Self {
        let (tx, rx) = lossy_channel::<Arc<Vec<u8>>>(queue_size);
        let is_live = Arc::new(AtomicBool::new(true));
        let join_handle = Some(std::thread::spawn({
            let name = name.clone();
            let tx_subscriber_gone = tx_subscriber_gone.clone();
            let is_live = Arc::clone(&is_live);
            move || {
                log::debug!(
                    "SenderSingleSubscriber, thread that writes to transport ({}): {:?}",
                    &name,
                    thread_priority::get_current_thread_priority()
                );
                let mut rx = rx;
                let mut transport = transport;
                let mut remainder = Option::<(usize, Arc<Vec<u8>>)>::None;
                loop {
                    if rx.kill_rx.try_recv().is_ok() {
                        log::debug!("got killrx, quitting ({})", &name);
                        break;
                    }
                    if let Some((ref mut idx, ref buf)) = remainder {
                        let num_bytes_total = buf.len();
                        match transport.write(&buf[*idx..]) {
                            Ok(num_bytes_written)
                                if num_bytes_total == *idx + num_bytes_written =>
                            {
                                remainder = None;
                            }
                            Ok(num_bytes_written) => {
                                *idx += num_bytes_total;
                                continue;
                            }
                            Err(e) => match e.kind() {
                                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
                                    continue;
                                }
                                other_error => {
                                    log::debug!("closing channel {:?} ({})", other_error, &name);
                                    break;
                                }
                            },
                        }
                    }
                    match rx.next() {
                        Some(serialized_msg) => {
                            remainder = Some((0, serialized_msg));
                        }
                        None => {
                            // channel closed
                            log::debug!("SingleSenderSubscriber, got None in rx.next()");
                            break;
                        }
                    }
                }
                if is_live.load(std::sync::atomic::Ordering::SeqCst) {
                    log::debug!("=> {} tx_subscriber_gone.send({})", &name, id);
                    log::debug!(
                        "tx_subscriber_gone.send(id={}) returned {:?} (name={})",
                        id,
                        tx_subscriber_gone.send(id),
                        &name
                    );
                    log::debug!("<= {} tx_subscriber_gone.send({})", &name, id);
                }
            }
        }));
        Self {
            id,
            tx,
            join_handle,
            name,
            tx_subscriber_gone,
            is_live,
            _transport_type: PhantomData,
        }
    }

    fn set_queue_size(&self, queue_size: usize) {
        self.tx.set_queue_size(queue_size);
    }

    fn set_queue_size_max(&self, queue_size: usize) {
        self.tx.set_queue_size_max(queue_size);
    }

    fn try_send(
        &self,
        serialized_msg: Arc<Vec<u8>>,
    ) -> Result<(), channel::TrySendError<Arc<Vec<u8>>>> {
        self.tx.try_send(serialized_msg)
    }
}

impl<T: Write + Send + 'static> Drop for SenderSingleSubscriber<T> {
    fn drop(&mut self) {
        let _ = self.tx.close();
        log::debug!(
            "closing SenderSingleSubscriber (→ {}), waiting for thread",
            &self.name
        );
        self.join_handle.take().unwrap().join().unwrap();
        log::debug!(
            "  ==> done closing SingleSenderSubscriber (→ {})",
            &self.name
        );
        if self.is_live.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = self.tx_subscriber_gone.send(self.id);
        }
    }
}

struct SubscriberInfo<T> {
    caller_id: String,
    stream: T,
}

impl<T> std::fmt::Debug for SubscriberInfo<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SubscriberInfo<{}> {{caller_id: \"{}\"}}",
            std::any::type_name::<T>(),
            &self.caller_id
        )
    }
}

#[derive(Clone, Copy)]
struct SetQueueSize {
    queue_size: usize,
    max: bool,
}

pub struct DataStream {
    target_names: Arc<Mutex<TargetNames>>,
    join_handle: Option<JoinHandle<()>>,
    // This is an Option, so that we can mem::take and drop the sender in the destructor, thereby
    // closing the channel, which in turn signals to our worker thread that it should terminate.
    tx_data: Option<Sender<Arc<Vec<u8>>>>,
    tx_queue_size: Sender<SetQueueSize>,
    topic_name: String,
}

impl DataStream {
    pub fn send(&self, data: Arc<Vec<u8>>) -> ForkResult {
        if let Err(e) = self.tx_data.as_ref().unwrap().send(data) {
            log::error!("error sending data on topic {}: {:?}", &self.topic_name, e);
        };
        Ok(())
    }

    #[inline]
    pub fn target_count(&self) -> usize {
        self.target_names.lock().expect(FAILED_TO_LOCK).count()
    }

    #[inline]
    pub fn target_names(&self) -> Vec<String> {
        self.target_names.lock().expect(FAILED_TO_LOCK).names()
    }

    #[inline]
    pub fn set_queue_size(&self, queue_size: usize) {
        if let Err(e) = self.tx_queue_size.send(SetQueueSize {
            max: false,
            queue_size,
        }) {
            log::error!("error setting queue size: {:?}", e);
        };
    }

    /// set the queue sizes of all senders to at least (!) `queue_size` (keep current setting if it's larger than `queue_size`)
    #[inline]
    pub fn set_queue_size_max(&self, queue_size: usize) {
        if let Err(e) = self.tx_queue_size.send(SetQueueSize {
            max: true,
            queue_size,
        }) {
            log::error!("error setting queue size (max): {:?}", e);
        };
    }
}

impl Drop for DataStream {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.tx_data));
        let names = self.target_names.lock().unwrap().names().clone();
        log::debug!("dropping Datastream (→{:?}), waiting for thread...", &names);
        self.join_handle.take().unwrap().join().unwrap();
        log::debug!("  done waiting for DataStream thread (→{:?})", names);
    }
}

#[derive(Debug, Default)]
pub struct TargetNames {
    targets: Vec<String>,
}

impl TargetNames {
    #[inline]
    pub fn count(&self) -> usize {
        self.targets.len()
    }

    #[inline]
    pub fn names(&self) -> Vec<String> {
        self.targets.clone()
    }
}
