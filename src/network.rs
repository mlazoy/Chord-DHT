use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use tokio::io::{AsyncReadExt, AsyncWriteExt};  
use tokio::runtime::Builder;  // For multi-threaded runtime

#[async_trait]
pub trait ConnectionHandler: Send + Sync {
    async fn handle_request(&self, stream: TcpStream)
    where
        Self: Send + Sync;  
}

pub struct Server<T: ConnectionHandler> {
    handler: Arc<T>,
}

impl<T: ConnectionHandler + 'static> Server<T> {
    pub fn new(handler: T) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    pub async fn wait_for_requests(&self, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let handler = Arc::clone(&self.handler);
                    tokio::spawn(async move {
                        handler.handle_request(stream).await;
                    });
                }
                Err(e) => {
                    eprintln!("Connection error: {}", e);
                }
            }
        }
    }
}
