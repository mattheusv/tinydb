use std::{env, path::Path};

use tinydb::{
    backend::Backend,
    catalog::pg_database,
    postgres_protocol::PostgresProtocol,
    sql::{ConnectionExecutor, ExecutorConfig},
    storage::{smgr::StorageManager, BufferPool},
};

#[tokio::main]
async fn main() {
    stderrlog::new()
        .module(module_path!())
        .verbosity(3)
        .init()
        .unwrap();

    let cwd = env::current_dir().expect("Failed to get current working directory");
    let data_dir = cwd.join("data");
    env::set_current_dir(Path::new(&data_dir)).unwrap();

    let buffer = BufferPool::new(120, StorageManager::new(&data_dir));

    env::set_current_dir(&data_dir).unwrap();

    let config = ExecutorConfig {
        database: pg_database::TINYDB_OID,
    };
    let conn_executor = ConnectionExecutor::new(config, buffer);
    let pgwire = PostgresProtocol::new(conn_executor);

    let backend = Backend::new(pgwire);
    if let Err(err) = backend.start().await {
        eprintln!("Failed to start backend: {}", err);
    }
}
