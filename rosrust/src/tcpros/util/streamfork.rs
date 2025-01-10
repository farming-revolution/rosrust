use crate::util::lossy_channel::{lossy_channel, LossyReceiver, LossySender};
use crate::util::FAILED_TO_LOCK;
use crossbeam::channel::{
    self, unbounded, bounded, Receiver, RecvError, Sender, TryRecvError, TrySendError,
};
use crossbeam::select;
use std::any::Any;
use std::io::Write;
use std::marker::PhantomData;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

pub fn fork<T: Write + Send + 'static>(
    queue_size: usize,
    topic: &str,
) -> (TargetList<T>, Arc<DataStream>) {
    let (tx_streams, rx_streams) = unbounded::<SubscriberInfo<T>>();
    let (tx_data, rx_data) = bounded::<Arc<Vec<u8>>>(0);
    let (tx_queue_size, rx_queue_size) = unbounded::<SetQueueSize>();

    let target_names = Arc::new(Mutex::new(TargetNames::default()));

    let join_handle = Some(thread::Builder::new()
        .name(format!("F{}", topic))
        .spawn({
            let target_names = Arc::clone(&target_names);
            move ||{ 
                let mut senders = Vec::new();
                let mut queue_size = queue_size;
                loop {
                    select! {
                        recv(rx_streams) -> new_conn_res => {
                            match new_conn_res {
                                Ok(new_connection) => {
                                    let sender = SenderSingleSubscriber::new(
                                        new_connection.stream,
                                        new_connection.caller_id.clone(),
                                        queue_size,
                                    );
                                    target_names
                                        .lock()
                                        .unwrap()
                                        .targets
                                        .push(new_connection.caller_id);
                                    senders.push(sender);
                                },
                                Err(crossbeam::channel::RecvError) => {
                                    return
                                }
                            }
                        },
                        recv(rx_data) -> data => {
                            let Ok(data) = data else {
                                return;
                            };
                            let mut disconnected_clients = Vec::new();
                            senders.retain(|sender| {
                                match sender.try_send(Arc::clone(&data)) {
                                    Ok(()) => true,
                                    Err(TrySendError::Full(_)) => {
                                        panic!("LossyChannel retured TrySendError::Full, this should not happen.");
                                    }
                                    Err(TrySendError::Disconnected(_)) => {
                                        // disconnected
                                        log::info!("client '{}' disconnected", &sender.name);
                                        disconnected_clients.push(sender.name.clone());
                                        false
                                    }
                                }
                            });

                            if !disconnected_clients.is_empty() {
                                let target_names = target_names.lock().unwrap();
                                target_names
                                    .names()
                                    .retain(|name| !disconnected_clients.contains(name));
                            }
                        },
                        recv(rx_queue_size) -> set_queue_size => {
                            let Ok(set_queue_size) = set_queue_size else {
                                // channel closed, quit loop
                                return;
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
            }
        })
        .expect("failed to spawn thread"));

    (
        TargetList(tx_streams),
        Arc::new(DataStream {
            target_names,
            join_handle,
            tx_data,
            tx_queue_size,
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
    tx: LossySender<Arc<Vec<u8>>>,
    join_handle: Option<JoinHandle<()>>,
    name: String,
    _transport_type: PhantomData<T>,
}

impl<T: Write + Send + 'static> SenderSingleSubscriber<T> {
    fn new(transport: T, name: String, queue_size: usize) -> Self {
        let (tx, rx) = lossy_channel::<Arc<Vec<u8>>>(queue_size);
        let join_handle = Some(std::thread::spawn({
            let name = name.clone();
            move || {
                let mut rx = rx;
                let mut transport = transport;
                loop {
                    match rx.next() {
                        Some(serialized_msg) => {
                            if let Err(e) = transport.write_all(serialized_msg.as_slice()) {
                                log::error!("error writing to transport to {}: {:?}", &name, e);
                            }
                        }
                        None => {
                            // channel closed
                            return;
                        }
                    }
                }
            }
        }));
        Self {
            tx,
            join_handle,
            name,
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
        self.join_handle.take().unwrap().join().unwrap();
    }
}

struct SubscriberInfo<T> {
    caller_id: String,
    stream: T,
}

impl<T> std::fmt::Debug for SubscriberInfo<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubscriberInfo<{}> {{caller_id: \"{}\"}}", std::any::type_name::<T>(), &self.caller_id)
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
    tx_data: Sender<Arc<Vec<u8>>>,
    tx_queue_size: Sender<SetQueueSize>,
}

impl DataStream {
    pub fn send(&self, data: Arc<Vec<u8>>) -> ForkResult {
        if let Err(e) = self.tx_data.send(data) {
            log::error!("error sending data"); // TODO: add topic name
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
        self.join_handle.take().unwrap().join().unwrap()
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
