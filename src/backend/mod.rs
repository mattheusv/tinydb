use std::{net::TcpListener, sync::Arc};

use tokio::task;

use crate::postgres_protocol::PostgresProtocol;

struct BackendState {
    pgwire: PostgresProtocol,
}
pub struct Backend(Arc<BackendState>);

impl Backend {
    pub fn new(pgwire: PostgresProtocol) -> Self {
        Self(Arc::new(BackendState { pgwire }))
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
        loop {
            let (mut socket, _) = listener.accept().unwrap();

            let backend = self.clone();
            task::spawn_blocking(move || match backend.0.pgwire.serve(&mut socket) {
                Ok(()) => {}
                Err(err) => {
                    println!("{}", err);
                    log::error!("Internal server error: {}", err)
                }
            });
        }
    }
}

impl Clone for Backend {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
