use std::fmt::Debug;
use crate::rosmsg::RosMsg;
#[cfg(not(target_arch = "wasm32"))]
use crate::Clock;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, atomic::AtomicUsize};

pub type ServiceResult<T> = Result<T, String>;

pub trait Message: Clone + Debug + Default + PartialEq + RosMsg + Send + Sync + 'static {
    fn msg_definition() -> String;
    fn md5sum() -> String;
    fn msg_type() -> String;
    #[cfg(not(target_arch = "wasm32"))]
    fn set_header(&mut self, _clock: &Arc<dyn Clock>, _seq: &Arc<AtomicUsize>) {}
}

pub trait ServicePair: Clone + Debug + Default + PartialEq + Message {
    type Request: RosMsg + Send + 'static;
    type Response: RosMsg + Send + 'static;
}

#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
    pub msg_type: String,
    pub md5sum: String,
}
