use std::env;

use tinydb::backend;
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stderrlog::new()
        .module(module_path!())
        .module("tinydb")
        .verbosity(3)
        .init()
        .unwrap();

    let cwd = env::current_dir().expect("Failed to get current working directory");
    let data_dir = cwd.join("data");
    env::set_current_dir(&data_dir).unwrap();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    backend::start(&data_dir, listener, signal::ctrl_c()).await;

    Ok(())
}
