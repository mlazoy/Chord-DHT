use std::sync::{Arc, Mutex};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;

pub trait ConnectionHandler: Send + Sync {
    fn handle_request(&mut self, stream: TcpStream);
}

pub struct Server<T: ConnectionHandler> {
    handler: Arc<Mutex<T>>, // Arc<Mutex<T>> allows mutable access across threads
}

impl<T: ConnectionHandler + 'static> Server<T> {
    pub fn new(handler: T) -> Self {
        Self {
            handler: Arc::new(Mutex::new(handler)),
        }
    }

    pub fn wait_for_requests(&self, listener: TcpListener, num_workers: usize) {
        let pool = ThreadPool::new(num_workers);
        for new_stream in listener.incoming() {
            match new_stream {
                Ok(stream) => {
                    let handler = Arc::clone(&self.handler);
                    pool.execute(move || {
                        // Lock the mutex to get mutable access
                        let mut handler = handler.lock().unwrap();
                        handler.handle_request(stream);
                    });
                }
                Err(e) => {
                    eprintln!("Connection error: {}", e);
                }
            }
        }
    }
}
