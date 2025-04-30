use super::error::{ErrorKind, Result};
use super::header::{self, decode};
use super::util::tcpconnection;
use super::{ServicePair, ServiceResult};
use crate::rosmsg::{encode_str, RosMsg};
use crate::RawMessage;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use error_chain::bail;
use log::error;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::{atomic, Arc};
use std::thread;

pub struct Service {
    pub api: String,
    pub msg_type: String,
    pub service: String,
    exists: Arc<atomic::AtomicBool>,
}

impl Drop for Service {
    fn drop(&mut self) {
        self.exists.store(false, atomic::Ordering::SeqCst);
    }
}

impl Service {
    pub fn new<T, F>(
        hostname: &str,
        bind_address: &str,
        port: u16,
        service: &str,
        node_name: &str,
        handler: F,
    ) -> Result<Service>
    where
        T: ServicePair,
        F: Fn(T::Request) -> ServiceResult<T::Response> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind((bind_address, port))?;
        let socket_address = listener.local_addr()?;
        let api = format!("rosrpc://{}:{}", hostname, socket_address.port());

        let service_exists = Arc::new(atomic::AtomicBool::new(true));

        let iterate_handler = {
            let service_exists = service_exists.clone();
            let service = String::from(service);
            let node_name = String::from(node_name);
            let handler = Arc::new(handler);
            move |stream: TcpStream| {
                if !service_exists.load(atomic::Ordering::SeqCst) {
                    return tcpconnection::Feedback::StopAccepting;
                }
                consume_client::<T, _, _>(&service, &node_name, Arc::clone(&handler), stream);
                tcpconnection::Feedback::AcceptNextStream
            }
        };

        tcpconnection::iterate(listener, format!("service '{}'", service), iterate_handler);

        Ok(Service {
            api,
            msg_type: T::msg_type(),
            service: String::from(service),
            exists: service_exists,
        })
    }
}

enum RequestType {
    Probe,
    Action,
}

fn consume_client<T, U, F>(service: &str, node_name: &str, handler: Arc<F>, mut stream: U)
where
    T: ServicePair,
    U: std::io::Read + std::io::Write + Send + 'static,
    F: Fn(T::Request) -> ServiceResult<T::Response> + Send + Sync + 'static,
{
    // Service request starts by exchanging connection headers
    match exchange_headers::<T, _>(&mut stream, service, node_name) {
        Err(err) => {
            // Connection can be closed when a client checks for a service.
            if !err.is_closed_connection() {
                error!(
                    "Failed to exchange headers for service '{}': {}",
                    service, err
                );
            }
        }
        // Spawn a thread for handling requests
        Ok(RequestType::Action) => spawn_request_handler::<T, U, F>(stream, Arc::clone(&handler)),
        Ok(RequestType::Probe) => (),
    }
}

fn exchange_headers<T, U>(stream: &mut U, service: &str, node_name: &str) -> Result<RequestType>
where
    T: ServicePair,
    U: std::io::Write + std::io::Read,
{
    let (req_type, requested_md5sum, requested_type) = read_request::<T, U>(stream, service)?;
    write_response::<T, U>(stream, node_name, requested_md5sum, requested_type)?;
    Ok(req_type)
}

fn read_request<T: ServicePair, U: std::io::Read>(
    stream: &mut U,
    service: &str,
) -> Result<(RequestType, String, String)> {
    let fields = header::decode(stream)?;
    header::match_field(&fields, "service", service)?;
    if fields.get("callerid").is_none() {
        bail!(ErrorKind::HeaderMissingField("callerid".into()));
    }
    if header::match_field(&fields, "probe", "1").is_ok() {
        return Ok((RequestType::Probe, "".to_owned(), "".to_owned()));
    }
    if TypeId::of::<T>() != TypeId::of::<RawMessage>() {
        header::match_field(&fields, "md5sum", &T::md5sum())?;
    }
    Ok((
        RequestType::Action,
        fields.get("md5sum").cloned().unwrap_or_else(|| "*".to_owned()),
        fields.get("type").cloned().unwrap_or_else(|| "*".to_owned())
    ))
}

fn write_response<T, U>(stream: &mut U, node_name: &str, requested_md5sum : String, requested_type: String) -> Result<()>
where
    T: ServicePair,
    U: std::io::Write,
{
    let mut fields = HashMap::<String, String>::new();
    fields.insert(String::from("callerid"), String::from(node_name));
    if TypeId::of::<T>() == TypeId::of::<RawMessage>() {
        // If we're operating as a RawMessage service, just tell the
        // client whatever it wants to hear and let our handler deal
        // with the fallout if that doesn't line up.
        fields.insert(String::from("md5sum"), requested_md5sum);
        fields.insert(String::from("type"), requested_type);
    } else {
        fields.insert(String::from("md5sum"), T::md5sum());
        fields.insert(String::from("type"), T::msg_type());
    }
    header::encode(stream, &fields)?;
    Ok(())
}

fn spawn_request_handler<T, U, F>(stream: U, handler: Arc<F>)
where
    T: ServicePair,
    U: std::io::Read + std::io::Write + Send + 'static,
    F: Fn(T::Request) -> ServiceResult<T::Response> + Send + Sync + 'static,
{
    thread::spawn(move || {
        if let Err(err) = handle_request_loop::<T, U, F>(stream, &handler) {
            if !err.is_closed_connection() {
                let info = err
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join("\nCaused by:");
                error!("{}", info);
            }
        }
    });
}

fn handle_request_loop<T, U, F>(mut stream: U, handler: &F) -> Result<()>
where
    T: ServicePair,
    U: std::io::Read + std::io::Write,
    F: Fn(T::Request) -> ServiceResult<T::Response>,
{
    // Receive request from client
    'request_loop: loop {
        let length = stream.read_u32::<LittleEndian>()?;
        let mut req_buf = vec![0u8; length];
        stream.read_exact(&mut req_buf)?;
        match RosMsg::decode(&req_buf) {
            Ok(req) => {
                // Call function that handles request and returns response
                match handler(req) {
                    Ok(res) => {
                        // Send True flag and response in case of success
                        stream.write_u8(1)?;
                        let mut writer = io::Cursor::new(Vec::with_capacity(128));
                        // skip the first 4 bytes that will contain the message length
                        writer.set_position(4);

                        res.encode(&mut writer)?;

                        // write the message length to the start of the header
                        let message_length = (writer.position() - 4) as u32;
                        writer.set_position(0);
                        message_length.encode(&mut writer)?;

                        stream.write_all(&writer.into_inner())?;
                    }
                    Err(message) => {
                        // Send False flag and error message string in case of failure
                        stream.write_u8(0)?;
                        RosMsg::encode(&message, &mut stream)?;
                    }
                };
            },
            Err(e) => {
                break 'request_loop;
            }
        }
    }

    // Upon failure to read request, send client failure message
    // This can be caused by actual issues or by the client stopping the connection
    stream.write_u8(0)?;
    encode_str("Failed to parse passed arguments", &mut stream)?;
    Ok(())
}
