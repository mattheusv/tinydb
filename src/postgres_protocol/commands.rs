#![cfg_attr(debug_assertions, allow(dead_code))]

use std::{
    collections::HashMap,
    io::{BufRead, Cursor},
};

use anyhow::{bail, Error};
use byteorder::{BigEndian, ByteOrder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    sql::{encode, PGResult, RowDescriptor},
    Oid,
};

pub const AUTH_TYPE_OK: u32 = 0;
pub const PROTOCOL_VERSION_NUMBER: u32 = 196608; // 3.0
pub const SSL_REQUEST_NUMBER: u32 = 80877103;
pub const CANCEL_REQUEST_CODE: u32 = 80877102;
pub const GSS_ENC_REQ_NUMBER: u32 = 80877104;

pub const PARSE_COMPLETE_TAG: u8 = b'1';
pub const BIND_COMPLETE_TAG: u8 = b'2';
pub const CLOSE_COMPLETE_TAG: u8 = b'3';
pub const NOTIFICATION_RESPONSE_TAG: u8 = b'A';
pub const COPY_DONE_TAG: u8 = b'c';
pub const COMMAND_COMPLETE_TAG: u8 = b'C';
pub const COPY_DATA_TAG: u8 = b'd';
pub const DATA_ROW_TAG: u8 = b'D';
pub const ERROR_RESPONSE_TAG: u8 = b'E';
pub const COPY_IN_RESPONSE_TAG: u8 = b'G';
pub const COPY_OUT_RESPONSE_TAG: u8 = b'H';
pub const EMPTY_QUERY_RESPONSE_TAG: u8 = b'I';
pub const BACKEND_KEY_DATA_TAG: u8 = b'K';
pub const NO_DATA_TAG: u8 = b'n';
pub const NOTICE_RESPONSE_TAG: u8 = b'N';
pub const AUTHENTICATION_TAG: u8 = b'R';
pub const PORTAL_SUSPENDED_TAG: u8 = b's';
pub const PARAMETER_STATUS_TAG: u8 = b'S';
pub const PARAMETER_DESCRIPTION_TAG: u8 = b't';
pub const ROW_DESCRIPTION_TAG: u8 = b'T';
pub const READY_FOR_QUERY_TAG: u8 = b'Z';

#[derive(Debug)]
pub enum Message {
    StartupMessage(StartupMessage),
    Query(Query),
    Terminate,
    ReadyForQuery,
    CommandComplete(String),
    RowDescriptor(RowDescriptor),
    AuthenticationOk,
    BackendKeyData,
    ParameterStatus(ParameterStatus),
    DataRow(PGResult),
    ErrorResponse(ErrorResponse),
}

#[derive(Debug)]
pub struct Query {
    pub query: String,
}

pub async fn decode<R>(decode_from: &mut R) -> anyhow::Result<Message>
where
    R: AsyncReadExt + std::marker::Unpin,
{
    let msg_type = decode_from.read_u8().await?;

    match msg_type {
        b'Q' => {
            let msg_len = decode_from.read_u32().await?;

            // Exclude the msg_len when reading
            let mut msg_body = vec![0; (msg_len as usize) - 4];
            decode_from.read(&mut msg_body).await?;

            // Exclude the \0 at the end when parsing.
            let _ = msg_body.pop();
            let query = String::from_utf8(msg_body)?;
            Ok(Message::Query(Query { query }))
        }
        b'X' => Ok(Message::Terminate),
        _ => anyhow::bail!("Message type {} not supported", msg_type),
    }
}

pub async fn encode<W>(encode_to: &mut W, message: Message) -> anyhow::Result<()>
where
    W: AsyncWriteExt + std::marker::Unpin,
{
    match message {
        Message::ReadyForQuery => {
            encode_to
                .write(&[READY_FOR_QUERY_TAG, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE_TAG])
                .await?;
            Ok(())
        }
        Message::CommandComplete(tag) => {
            encode_to.write_u8(COMMAND_COMPLETE_TAG).await?;
            encode_to.write_i32((tag.len() as i32) + 5).await?;
            encode_to.write(&tag.as_bytes()).await?;
            encode_to.write_u8(0).await?;
            Ok(())
        }
        Message::RowDescriptor(desc) => {
            let mut field_values = Vec::new();

            field_values.write_u16(desc.fields.len() as u16).await?;
            for field in &desc.fields {
                field_values.write(&field.name).await?;
                field_values.write_u8(0).await?;

                field_values.write_u32(field.table_oid).await?;
                field_values.write_u16(field.table_attribute_number).await?;
                field_values.write_u32(field.data_type_oid).await?;
                field_values.write_i16(field.data_type_size).await?;
                field_values.write_i32(field.type_modifier).await?;
                field_values.write_i16(field.format).await?;
            }

            encode_to.write_u8(ROW_DESCRIPTION_TAG).await?;
            encode_to.write_i32((field_values.len() as i32) + 4).await?;
            encode_to.write(&field_values).await?;
            Ok(())
        }
        Message::AuthenticationOk => {
            encode_to.write(&[AUTHENTICATION_TAG]).await?;
            encode_to.write_i32(8).await?;
            encode_to.write_u32(AUTH_TYPE_OK).await?;
            Ok(())
        }
        Message::BackendKeyData => {
            encode_to.write_u8(BACKEND_KEY_DATA_TAG).await?;
            // message lenght
            encode_to.write_u32(12).await?;
            // process id
            encode_to.write_u32(42).await?;
            // secret key
            encode_to.write_u32(12345).await?;
            Ok(())
        }
        Message::ParameterStatus(status) => {
            let mut buf = Vec::new();
            buf.write(status.key.as_bytes()).await?;
            buf.write_u8(0).await?;
            buf.write(status.value.as_bytes()).await?;
            buf.write_u8(0).await?;

            encode_to.write_u8(PARAMETER_STATUS_TAG).await?;
            encode_to.write_i32((buf.len() as i32) + 4).await?;
            encode_to.write(&buf).await?;
            Ok(())
        }
        Message::DataRow(result) => {
            let mut data_rows = Vec::new();

            for row in result.tuples {
                let row = row.iter();
                let mut buf_row = Vec::new();

                buf_row.write_u16(row.len() as u16).await?;
                for (attnum, datum) in row.enumerate() {
                    match datum {
                        Some(datum) => match &result.desc.fields.get(attnum) {
                            Some(att_desc) => {
                                let datum = encode::decode(datum, att_desc.data_type_oid as Oid)?;
                                let datum = datum.as_bytes();
                                buf_row.write_u32(datum.len() as u32).await?;
                                buf_row.write(datum).await?;
                            }
                            None => {
                                bail!("Can not find field desc for attnum {}", attnum)
                            }
                        },
                        None => {
                            buf_row.write_u32(0).await?;
                        }
                    }
                }
                data_rows.write_u8(DATA_ROW_TAG).await?;
                data_rows.write_i32((buf_row.len() as i32) + 4).await?;
                data_rows.write(&buf_row).await?;
            }

            encode_to.write(&data_rows).await?;

            Ok(())
        }
        Message::StartupMessage(_) | Message::Query(_) => {
            bail!("can not encode message {:?}", message)
        }
        Message::ErrorResponse(err) => {
            encode_to.write_u8(ERROR_RESPONSE_TAG).await?;
            let mut buf = Vec::new();

            buf.write_u8(b'M').await?;
            buf.write(&err.error.to_string().as_bytes()).await?;
            buf.write_u8(0).await?;

            // Mark the the end of error response.
            buf.write_u8(0).await?;

            encode_to.write_u32((buf.len() + 4) as u32).await?;
            encode_to.write(&buf).await?;
            Ok(())
        }
        Message::Terminate => Ok(()),
    }
}

#[derive(Debug)]
pub struct ParameterStatus {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub error: Error,
}

#[derive(Debug)]
pub struct StartupMessage {
    pub protocol_version: u32,
    pub parameters: HashMap<String, String>,
}

impl StartupMessage {
    pub fn decode(src: &[u8]) -> anyhow::Result<Self> {
        if src.len() < 4 {
            anyhow::bail!("startup message to short");
        }

        let protocol_version = BigEndian::read_u32(src);

        let mut parameters = HashMap::new();

        let mut cursor = Cursor::new(&src[4..]);
        loop {
            let mut buf = Vec::new();
            cursor.read_until(0, &mut buf)?;
            if buf.is_empty() {
                break;
            }

            let _ = buf.pop(); // Remove \0
            let key = String::from_utf8(buf)?;

            let mut buf = Vec::new();
            cursor.read_until(0, &mut buf)?;
            let _ = buf.pop(); // Remove \0
            let value = String::from_utf8(buf)?;

            parameters.insert(key, value);
        }

        Ok(Self {
            protocol_version,
            parameters,
        })
    }
}
