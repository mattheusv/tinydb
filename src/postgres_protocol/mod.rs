pub mod commands;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

use anyhow::Result;

use crate::sql::PGResult;

use self::commands::{Message, StartupMessage, PROTOCOL_VERSION_NUMBER, SSL_REQUEST_NUMBER};

/// Connection implements the Postgres wire protocol (version 3 of the protocol, implemented
/// by Postgres 7.4 an later). receive() reads protocol messages, and return a Message type
/// to be executed by connection handler.
///
/// The connection handler execute the commands returned by receive() method and use the
/// connection to send the appropriate messages back to the client.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream` used to read and write data back and from the client.
    stream: TcpStream,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection { stream: socket }
    }

    /// Read a single message from tcp stream.
    ///
    /// The function waits until it has retrieved enough data to parse a message.
    pub fn receive(&mut self) -> Result<Message> {
        let message = commands::decode(&mut self.stream)?;
        Ok(message)
    }

    /// Send a query result back to the client.
    pub fn send_result(&mut self, result: PGResult) -> Result<()> {
        let rows = result.tuples.len();

        commands::encode(
            &mut self.stream,
            Message::RowDescriptor(result.desc.clone()),
        )?;
        commands::encode(&mut self.stream, Message::DataRow(result))?;
        self.command_complete(&format!("SELECT {}", rows))?;
        Ok(())
    }

    /// Send to the client that the command returned by receive() is completed.
    pub fn command_complete(&mut self, tag: &str) -> Result<()> {
        commands::encode(
            &mut self.stream,
            Message::CommandComplete(String::from(tag)),
        )?;
        commands::encode(&mut self.stream, Message::ReadyForQuery)?;
        Ok(())
    }

    pub fn handle_startup_message(&mut self) -> Result<()> {
        let message = self.receive_startup_message()?;
        match message {
            Message::StartupMessage { .. } => {
                commands::encode(&mut self.stream, Message::AuthenticationOk)?;
                commands::encode(&mut self.stream, Message::ReadyForQuery)?;
            }
            _ => anyhow::bail!("Unexpected message type to handle on startup"),
        }
        Ok(())
    }

    fn receive_startup_message(&mut self) -> Result<Message> {
        let msg_size = self.stream.read_u32::<BigEndian>()? - 4;

        let mut buf = vec![0; msg_size as usize];
        self.stream.read(&mut buf)?;
        let code = BigEndian::read_u32(&buf);

        match code {
            PROTOCOL_VERSION_NUMBER => StartupMessage::decode(&buf),
            SSL_REQUEST_NUMBER => {
                self.stream.write(&"N".as_bytes())?;
                self.receive_startup_message()
            }
            _ => anyhow::bail!("Unexpected startup code: {}", code),
        }
    }
}
