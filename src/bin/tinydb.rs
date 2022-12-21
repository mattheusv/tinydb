use std::env;

use structopt::StructOpt;
use tinydb::{
    backend,
    cli::Flags,
    initdb::init_database,
    storage::{smgr::StorageManager, BufferPool},
};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::from_args();

    stderrlog::new()
        .module(module_path!())
        .quiet(flags.quiet)
        .timestamp(stderrlog::Timestamp::Second)
        .verbosity(flags.verbose)
        .init()
        .unwrap();

    let cwd = env::current_dir().expect("Failed to get current working directory");
    let data_dir = cwd.join(&flags.data_dir);

    if flags.init {
        log::info!("Initializing database directory");
        let buffer = BufferPool::new(120, StorageManager::new(&data_dir));
        init_database(&buffer, &data_dir).expect("Failed init default database");
        log::info!("Database directory initialized");
    }

    env::set_current_dir(&data_dir).unwrap();

    let listener = TcpListener::bind(format!("{}:{}", flags.hostname, flags.port)).await?;

    let config = backend::Config {
        data_dir,
        buffer_pool_size: 120,
    };

    log::info!("starting tinydb server");
    log::info!(
        "listening on address {}, port {}",
        flags.hostname,
        flags.port
    );
    backend::start(&config, listener, signal::ctrl_c()).await;
    log::info!("database system is shut down");

    Ok(())
}
