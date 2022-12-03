use std::{env, sync::Arc};

use tinydb::{
    backend::Backend,
    catalog::pg_database,
    sql::{ConnectionExecutor, ExecutorConfig},
    storage::{smgr::StorageManager, BufferPool},
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    stderrlog::new()
        .module(module_path!())
        .module("tinydb")
        .verbosity(3)
        .init()
        .unwrap();

    let cwd = env::current_dir().expect("Failed to get current working directory");
    let data_dir = cwd.join("data");
    env::set_current_dir(&data_dir).unwrap();

    let buffer = BufferPool::new(120, StorageManager::new(&data_dir));

    let config = ExecutorConfig {
        database: pg_database::TINYDB_OID,
    };
    let conn_executor = Arc::new(ConnectionExecutor::new(config, buffer));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let backend = Backend::new(listener, conn_executor);

    if let Err(err) = backend.start().await {
        eprintln!("Failed to start backend: {}", err);
    }
}
