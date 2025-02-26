#![recursion_limit = "1024"]

#[cfg(not(target_arch = "wasm32"))]
pub use crate::api::raii::{Publisher, Service, Subscriber};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::api::handlers::{SubscriptionHandler};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::api::{error, Clock, Parameter};
pub use crate::raw_message::{RawMessage, RawMessageDescription};
#[doc(hidden)]
pub use crate::rosmsg::RosMsg;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::singleton::*;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::tcpros::{Client, ClientResponse, Message, ServicePair};
pub use dynamic_msg::DynamicMsg;
pub use ros_message::{Duration, MessageValue as MsgMessage, Time, Value as MsgValue};
#[doc(hidden)]
pub use rosrust_codegen::*;
#[cfg(not(target_arch = "wasm32"))]
pub mod wall_time;

pub mod common;

#[cfg(target_arch = "wasm32")] // when using std, these are exported by "pub use crate::tcpros::…" above
pub use common::{Message, ServicePair};

#[cfg(not(target_arch = "wasm32"))]
pub mod api;
mod dynamic_msg;
mod log_macros;
#[doc(hidden)]
pub mod msg;
mod raw_message;
#[doc(hidden)]
pub mod rosmsg;
#[cfg(not(target_arch = "wasm32"))]
mod rosxmlrpc;
#[cfg(not(target_arch = "wasm32"))]
pub mod singleton;
#[cfg(not(target_arch = "wasm32"))]
mod tcpros;
#[cfg(not(target_arch = "wasm32"))]
mod util;

#[cfg(target_arch = "wasm32")]
pub mod nostd_error;
#[cfg(target_arch = "wasm32")]
pub use nostd_error as error;
