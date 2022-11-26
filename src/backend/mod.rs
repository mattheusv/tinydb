use anyhow::{bail, Result};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use std::{net::TcpListener, sync::Arc};

use tokio::task;

use crate::{
    postgres_protocol::{commands::Message, Connection},
    sql::{ConnectionExecutor, SQLError},
};

/// Backend TCP listener. It includes a `start` method which performs the TCP listening and
/// initialization of per-connection state.
pub struct Backend {
    /// TCP listener supplied by the `start` caller.
    listener: TcpListener,

    /// Shared connection executor, it execute incomming SQL comands.
    conn_executor: Arc<ConnectionExecutor>,
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

    /// Shared database connection executor.
    ///
    /// When a command is received from `connection`, it is executed with `conn_executor`.
    conn_executor: Arc<ConnectionExecutor>,
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
    fn run(&mut self) -> Result<()> {
        self.connection.handle_startup_message()?;
        log::info!("New connection accepted");
        loop {
            let msg = self.connection.receive()?;

            match msg {
                Message::Query(query) => {
                    let ast = Parser::parse_sql(&DIALECT, &query.query)?;
                    for stmt in ast {
                        match stmt {
                            Statement::Query(query) => {
                                let result = self.conn_executor.exec_pg_query(&query)?;
                                self.connection.send_result(result)?;
                            }
                            Statement::Insert {
                                table_name,
                                columns,
                                source,
                                ..
                            } => {
                                self.conn_executor
                                    .exec_insert(&table_name, &columns, &source)?;
                                self.connection.command_complete(&"INSERT")?;
                            }
                            Statement::CreateTable { name, columns, .. } => {
                                self.conn_executor.exec_create_table(&name, &columns)?;
                                self.connection.command_complete(&"CREATE")?;
                            }
                            _ => bail!(SQLError::Unsupported(stmt.to_string())),
                        }
                    }
                }
                _ => anyhow::bail!("Unexpected message type to handle"),
            };
        }
    }
}

impl Backend {
    /// Create a new backend using the given listener to accpet incoming tcp connections. The given
    /// connection executor is shared with all handled connections.
    pub fn new(listener: TcpListener, conn_executor: Arc<ConnectionExecutor>) -> Self {
        Self {
            listener,
            conn_executor,
        }
    }

    /// Start the backend.
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    pub fn start(&self) -> Result<()> {
        log::info!("Backend started, accepting inbound connections");
        loop {
            let (socket, _) = self.listener.accept()?;

            let mut handler = Handler {
                connection: Connection::new(socket),
                conn_executor: self.conn_executor.clone(),
            };
            task::spawn_blocking(move || {
                if let Err(err) = handler.run() {
                    log::error!("connection serve error: {}", err);
                }
            });
        }
    }
}
