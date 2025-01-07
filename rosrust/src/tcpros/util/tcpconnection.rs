use log::error;
use std::net::{TcpListener, TcpStream};
use std::thread;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Feedback {
    AcceptNextStream,
    StopAccepting,
}

pub fn iterate<F>(listener: TcpListener, tag: String, handler: F) -> thread::JoinHandle<()>
where
    F: Fn(TcpStream) -> Feedback + Send + 'static,
{
    let handle = thread::Builder::new()
        .name(format!("L{}", &tag))
        .spawn(move || listener_thread(&listener, &tag, handler));
    handle.expect("failed to spawn thread")
}

fn listener_thread<F>(connections: &TcpListener, tag: &str, handler: F)
where
    F: Fn(TcpStream) -> Feedback + Send + 'static,
{
    for stream in connections.incoming() {
        match stream {
            Ok(stream) => {
                // let timeout = std::time::Duration::from_secs(1);
                // stream.set_write_timeout(Some(timeout)).unwrap();
                // stream.set_read_timeout(Some(timeout)).unwrap();
                let debug_msg = format!(
                    "t: {} | tag: {} | peer: {:?} | read to: {:?} | write to: {:?}",
                    crate::now(), &tag, stream.peer_addr(), stream.read_timeout(), stream.write_timeout()
                ).replace('\n', "");
                println!("{}", debug_msg);
                match handler(stream) {
                    Feedback::AcceptNextStream => {}
                    Feedback::StopAccepting => break,
                }
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::InvalidInput {
                    // Got EINVAL, quitting listener thread. This happens after
                    // Publisher.drop() calls libc::shutdown on the TcpListener's
                    // file descriptor. When the publisher is dropped, we can stop
                    // accepting new connections here, and end the thread.
                    return;
                }
                error!("TCP connection failed at {}: {}", &tag, &err);
            }
        }
    }
}
