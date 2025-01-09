use crate::util::lossy_channel::{lossy_channel, LossyReceiver, LossySender};
use crate::util::FAILED_TO_LOCK;
use crossbeam::channel::{
    self, unbounded, Receiver, RecvError, Sender, TryRecvError, TrySendError,
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
    let (tx_data, rx_data) = unbounded::<Arc<Vec<u8>>>();
    let (tx_queue_size, rx_queue_size) = unbounded::<SetQueueSize>();

    // let mut fork_thread = ForkThread::new();
    // let target_names = Arc::clone(&fork_thread.target_names);
    let target_names = Arc::new(Mutex::new(TargetNames::default()));

    let join_handle = Some(thread::Builder::new()
        .name(format!("F{}", topic))
        .spawn({
            let target_names = Arc::clone(&target_names);
            move ||{ 
                let mut senders = Vec::new();
                let mut queue_size = queue_size;
            
                loop {

                    dbg!(rx_streams.is_empty());

                    select! {
                        recv(rx_streams) -> new_conn_res => {
                            dbg!("got rx_streams!");
                            match new_conn_res {
                                Ok(new_connection) => {
                                    dbg!(&new_connection);
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
                                    dbg!(&senders.len());
                                },
                                Err(crossbeam::channel::RecvError) => {
                                    dbg!("rx_streams RecvError");
                                    return
                                }
                            }
                        },
                        recv(rx_data) -> data => {
                            dbg!("got data!");
                            let Ok(data) = data else {
                                return;
                            };
                            eprintln!("sending to {} senders: {}", senders.len(), String::from_utf8_lossy(data.as_slice()));
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

// struct ForkThread<T: Write + Send + 'static> {
//     targets: Vec<SubscriberInfo<T>>,
//     target_names: Arc<Mutex<TargetNames>>,
// }

// impl<T: Write + Send + 'static> ForkThread<T> {
//     // pub fn new() -> Self {
//     //     Self {
//     //         targets: vec![],
//     //         target_names: Arc::new(Mutex::new(TargetNames {
//     //             targets: Vec::new(),
//     //         })),
//     //     }
//     // }

//     fn publish_buffer_and_prune_targets(&mut self, buffer: &[u8]) {
//         let mut dropped_targets = vec![];
//         for (idx, target) in self.targets.iter_mut().enumerate() {

//             if let Err(e) = target.stream.write_all(buffer) {
//                 dropped_targets.push(idx);
//                 dbg!((e, &target.caller_id));
//                 let s = &target.stream as &dyn Any;
//                 if let Some(s) = s.downcast_ref::<TcpStream>() {
//                     dbg!(s.peer_addr());
//                 }
//             }
//         }

//         if !dropped_targets.is_empty() {
//             // We reverse the order, to remove bigger indices first.
//             for idx in dropped_targets.into_iter().rev() {
//                 self.targets.swap_remove(idx);
//             }
//             self.update_target_names();
//         }
//     }

//     fn add_target(&mut self, target: SubscriberInfo<T>) {
//         self.targets.push(target);
//         self.update_target_names();
//     }

//     fn update_target_names(&self) {
//         let targets = self
//             .targets
//             .iter()
//             .map(|target| target.caller_id.clone())
//             .collect();
//         *self.target_names.lock().expect(FAILED_TO_LOCK) = TargetNames { targets };
//     }

//     fn step(
//         &mut self,
//         streams: &Receiver<SubscriberInfo<T>>,
//         data: &LossyReceiver<Arc<Vec<u8>>>,
//     ) -> Result<(), channel::RecvError> {
//         channel::select! {
//             recv(data.kill_rx.kill_rx) -> msg => {
//                 return msg.and(Err(channel::RecvError));
//             }
//             recv(data.data_rx) -> msg => {
//                 self.publish_buffer_and_prune_targets(&msg?);
//             }
//             recv(streams) -> target => {
//                 self.add_target(target?);
//             }
//         }
//         Ok(())
//     }

//     pub fn run(
//         &mut self,
//         streams: &Receiver<SubscriberInfo<T>>,
//         data: &LossyReceiver<Arc<Vec<u8>>>,
//     ) {
//         while self.step(streams, data).is_ok() {}
//     }
// }

pub type ForkResult = Result<(), ()>;

pub struct TargetList<T: Write + Send + 'static>(Sender<SubscriberInfo<T>>);

impl<T: Write + Send + 'static> TargetList<T> {
    pub fn add(&self, caller_id: String, stream: T) -> ForkResult {
        self.0
            .send(SubscriberInfo { caller_id, stream })
            .or(Err(()))
    }
}

impl<T: Write + Send + 'static> Drop for TargetList<T> {
    fn drop(&mut self) {
        eprintln!("dropping TargetList!");
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
        write!(f, "SubscriberInfo<> {{caller_id: \"{}\"}}", &self.caller_id)
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
