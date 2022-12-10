use crate::{
    catalog::pg_database,
    postgres_protocol::{commands::Message, Connection},
    sql::{ConnectionExecutor, ExecutorConfig, SQLError},
    storage::{smgr::StorageManager, BufferPool},
};
use anyhow::{bail, Result};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use std::{future::Future, path::PathBuf};
use tokio::{net::TcpListener, task};

/// Backend TCP listener. It includes a `start` method which performs the TCP listening and
/// initialization of per-connection state.
pub struct Backend {
    /// TCP listener supplied by the `start` caller.
    listener: TcpListener,

    /// Shared buffer pool used by all connection handlers.
    buffer_pool: BufferPool,
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// SQL commands using conn_executor.
struct Handler {
    /// The TCP connection decorated with the postgres protocol encoder / decoder.
    ///
    /// When `Backend` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`.
    ///
    /// `Connection` allows the handler to operate at the "message" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// Database connection executor. A connection executor for each connection handler.
    ///
    /// When a command is received from `connection`, it is executed with `conn_executor`.
    conn_executor: ConnectionExecutor,
}

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

impl Handler {
    /// Process a single connection.
    ///
    /// Request message are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Before starting executing SQL commands the startup message is handled by
    /// `run` method.
    async fn run(&mut self) -> Result<()> {
        self.connection.handle_startup_message().await?;
        log::info!("New connection accepted");
        loop {
            let msg = self.connection.receive().await?;
            if let Message::Terminate = msg {
                log::info!("Closing connection with {}", self.connection.peer_addr()?);
                return Ok(());
            }

            if let Err(err) = self.exec_message(msg).await {
                self.connection.send_error(err).await?;
                self.connection.ready_for_query().await?;
            }
        }
    }

    async fn exec_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Query(query) => {
                let ast = Parser::parse_sql(&DIALECT, &query.query)?;
                for stmt in ast {
                    match stmt {
                        Statement::Query(query) => {
                            let result = self.conn_executor.exec_query(&query)?;
                            self.connection.send_result(result).await?;
                        }
                        Statement::Insert {
                            table_name,
                            columns,
                            source,
                            ..
                        } => {
                            self.conn_executor
                                .exec_insert(&table_name, &columns, &source)?;
                            self.connection.command_complete(&"INSERT").await?;
                        }
                        Statement::CreateTable { name, columns, .. } => {
                            self.conn_executor.exec_create_table(&name, &columns)?;
                            self.connection.command_complete(&"CREATE").await?;
                        }
                        _ => bail!(SQLError::Unsupported(stmt.to_string())),
                    }
                }
            }
            _ => anyhow::bail!("Unexpected message type to execute"),
        };
        Ok(())
    }
}

impl Backend {
    /// Create a new backend using the given listener to accept incoming tcp connections. The given
    /// buffer pool is shared with all connections handlers.
    pub fn new(listener: TcpListener, buffer_pool: BufferPool) -> Self {
        Self {
            listener,
            buffer_pool,
        }
    }

    /// Start the backend.
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    pub async fn start(&self) -> Result<()> {
        log::info!("Backend started, accepting inbound connections");
        loop {
            let (socket, _) = self.listener.accept().await?;

            // TODO: Read the database from startup message parameters.
            let config = ExecutorConfig {
                database: pg_database::TINYDB_OID,
            };

            let mut handler = Handler {
                connection: Connection::new(socket),
                conn_executor: ConnectionExecutor::new(config, self.buffer_pool.clone()),
            };
            task::spawn(async move {
                if let Err(err) = handler.run().await {
                    log::error!("connection serve error: {}", err);
                }
            });
        }
    }
}

/// Backend server configuration options.
pub struct Config {
    /// Absolute path to PGDATA directory.
    pub data_dir: PathBuf,

    /// Size of buffer pool.
    pub buffer_pool_size: usize,
}

/// Start the tinydb backend server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn start(config: &Config, listener: TcpListener, shutdown: impl Future) {
    let buffer = BufferPool::new(
        config.buffer_pool_size,
        StorageManager::new(&config.data_dir),
    );

    let backend = Backend::new(listener, buffer);

    tokio::select! {
        res = backend.start() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                log::error!("Failed to accept connection: {}", err);
            }
        }
        _ = shutdown => {
            // Shutdown signal has been received.
            //
            // The buffer pool will be droped at this point that will force all
            // in memory dirty pages to be written on disk.
            log::info!("Shutting down the backend");
        }
    }
}
