mod commands;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};

use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::{postgres_protocol::commands::ReadyForQuery, sql::ConnectionExecutor};

use self::commands::{
    AuthenticationOk, CommandComplete, FrontendMessage, Query, StartupMessage,
    PROTOCOL_VERSION_NUMBER, SSL_REQUEST_NUMBER,
};

/// PostgresProtocol implements the Postgres wire protocol (version 3 of the protocol, implemented
/// by Postgres 7.4 an later). serve() reads protocol messages, transforms them into SQL commands
/// that is seended to connection executor handler.
///
/// The connectior executor produces results for the SQL commands, which are delivered to the
/// client.
pub struct PostgresProtocol {
    connection_executor: ConnectionExecutor,
}

impl PostgresProtocol {
    pub fn new(connection_executor: ConnectionExecutor) -> Self {
        Self {
            connection_executor,
        }
    }

    /// Continuously reads from the network stream connection and pushes execution instructions to
    /// connection executor. The method returns when the pgwrite termination message is received.
    pub fn serve(&self, socket: &mut TcpStream) -> anyhow::Result<()> {
        self.handle_startup_message(socket)?;

        loop {
            self.handle_message(socket)?;
        }
    }

    fn handle_message(&self, socket: &mut TcpStream) -> anyhow::Result<()> {
        let message = self.receive(socket)?;
        match message {
            FrontendMessage::Query(query) => {
                println!("{}", query.query);
                ReadyForQuery::encode(socket)?;

                Ok(())
            }
            _ => anyhow::bail!("Unexpected message type to handle"),
        }
    }

    fn receive(&self, socket: &mut TcpStream) -> anyhow::Result<FrontendMessage> {
        let msg_type = socket.read_u8()?;

        match msg_type {
            b'Q' => Ok(Query::decode(socket)?),
            _ => anyhow::bail!("Message type {} not supported", msg_type),
        }
    }

    fn receive_startup_message(&self, socket: &mut TcpStream) -> anyhow::Result<FrontendMessage> {
        let msg_size = socket.read_u32::<BigEndian>()? - 4;

        let mut buf = vec![0; msg_size as usize];
        socket.read(&mut buf)?;
        let code = BigEndian::read_u32(&buf);

        match code {
            PROTOCOL_VERSION_NUMBER => StartupMessage::decode(&buf),
            SSL_REQUEST_NUMBER => {
                socket.write(&"N".as_bytes())?;
                self.receive_startup_message(socket)
            }
            _ => anyhow::bail!("Unexpected startup code: {}", code),
        }
    }

    fn handle_startup_message(&self, socket: &mut TcpStream) -> anyhow::Result<()> {
        let message = self.receive_startup_message(socket)?;
        match message {
            FrontendMessage::StartupMessage { .. } => {
                AuthenticationOk::encode(socket)?;
                ReadyForQuery::encode(socket)?;
            }
            _ => anyhow::bail!("Unexpected message type to handle on startup"),
        }
        Ok(())
    }
}
