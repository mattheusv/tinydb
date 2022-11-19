#![cfg_attr(debug_assertions, allow(dead_code))]

use std::{
    collections::HashMap,
    io::{BufRead, Cursor, Write},
};

use anyhow::bail;
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

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
    ReadyForQuery,
    CommandComplete(String),
    RowDescriptor(RowDescriptor),
    AuthenticationOk,
    BackendKeyData,
    ParameterStatus(ParameterStatus),
    DataRow(PGResult),
}

#[derive(Debug)]
pub struct Query {
    pub query: String,
}

pub fn decode<R>(decode_from: &mut R) -> anyhow::Result<Message>
where
    R: byteorder::ReadBytesExt,
{
    let msg_type = decode_from.read_u8()?;

    match msg_type {
        b'Q' => {
            let msg_len = decode_from.read_u32::<BigEndian>()?;

            // Exclude the msg_len when reading
            let mut msg_body = vec![0; (msg_len as usize) - 4];
            decode_from.read(&mut msg_body)?;

            // Exclude the \0 at the end when parsing.
            let _ = msg_body.pop();
            let query = String::from_utf8(msg_body)?;
            Ok(Message::Query(Query { query }))
        }
        _ => anyhow::bail!("Message type {} not supported", msg_type),
    }
}

pub fn encode<W>(encode_to: &mut W, message: Message) -> anyhow::Result<()>
where
    W: Write,
{
    match message {
        Message::ReadyForQuery => {
            encode_to.write(&[READY_FOR_QUERY_TAG, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE_TAG])?;
            Ok(())
        }
        Message::CommandComplete(tag) => {
            encode_to.write_u8(COMMAND_COMPLETE_TAG)?;
            encode_to.write_i32::<BigEndian>((tag.len() as i32) + 5)?;
            encode_to.write(&tag.as_bytes())?;
            encode_to.write_u8(0)?;
            Ok(())
        }
        Message::RowDescriptor(desc) => {
            let mut field_values = Vec::new();

            field_values.write_u16::<BigEndian>(desc.fields.len() as u16)?;
            for field in &desc.fields {
                field_values.write(&field.name)?;
                field_values.write_u8(0)?;

                field_values.write_u32::<BigEndian>(field.table_oid)?;
                field_values.write_u16::<BigEndian>(field.table_attribute_number)?;
                field_values.write_u32::<BigEndian>(field.data_type_oid)?;
                field_values.write_i16::<BigEndian>(field.data_type_size)?;
                field_values.write_i32::<BigEndian>(field.type_modifier)?;
                field_values.write_i16::<BigEndian>(field.format)?;
            }

            encode_to.write_u8(ROW_DESCRIPTION_TAG)?;
            encode_to.write_i32::<BigEndian>((field_values.len() as i32) + 4)?;
            encode_to.write(&field_values)?;
            Ok(())
        }
        Message::AuthenticationOk => {
            encode_to.write(&[AUTHENTICATION_TAG])?;
            encode_to.write_i32::<BigEndian>(8)?;
            encode_to.write_u32::<BigEndian>(AUTH_TYPE_OK)?;
            Ok(())
        }
        Message::BackendKeyData => {
            encode_to.write_u8(BACKEND_KEY_DATA_TAG)?;
            // message lenght
            encode_to.write_u32::<BigEndian>(12)?;
            // process id
            encode_to.write_u32::<BigEndian>(42)?;
            // secret key
            encode_to.write_u32::<BigEndian>(12345)?;
            Ok(())
        }
        Message::ParameterStatus(status) => {
            let mut buf = Vec::new();
            buf.write(status.key.as_bytes())?;
            buf.write_u8(0)?;
            buf.write(status.value.as_bytes())?;
            buf.write_u8(0)?;

            encode_to.write_u8(PARAMETER_STATUS_TAG)?;
            encode_to.write_i32::<BigEndian>((buf.len() as i32) + 4)?;
            encode_to.write(&buf)?;
            Ok(())
        }
        Message::DataRow(result) => {
            let mut data_rows = Vec::new();

            for row in result.tuples {
                let row = row.iter();
                let mut buf_row = Vec::new();

                buf_row.write_u16::<BigEndian>(row.len() as u16)?;
                for (attnum, datum) in row.enumerate() {
                    match datum {
                        Some(datum) => match &result.desc.fields.get(attnum) {
                            Some(att_desc) => {
                                let datum = encode::decode(datum, att_desc.data_type_oid as Oid)?;
                                let datum = datum.as_bytes();
                                buf_row.write_u32::<BigEndian>(datum.len() as u32)?;
                                buf_row.write(datum)?;
                            }
                            None => {
                                bail!("Can not find field desc for attnum {}", attnum)
                            }
                        },
                        None => {
                            buf_row.write_u32::<BigEndian>(0)?;
                        }
                    }
                }
                data_rows.write_u8(DATA_ROW_TAG)?;
                data_rows.write_i32::<BigEndian>((buf_row.len() as i32) + 4)?;
                data_rows.write(&buf_row)?;
            }

            encode_to.write(&data_rows)?;

            Ok(())
        }
        Message::StartupMessage(_) | Message::Query(_) => {
            bail!("can not encode message {:?}", message)
        }
    }
}

#[derive(Debug)]
pub struct ParameterStatus {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct StartupMessage {
    pub protocol_version: u32,
    pub parameters: HashMap<String, String>,
}

impl StartupMessage {
    pub fn decode(src: &[u8]) -> anyhow::Result<Message> {
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

            let key = String::from_utf8(buf)?;

            let mut buf = Vec::new();
            cursor.read_until(0, &mut buf)?;
            let value = String::from_utf8(buf)?;

            parameters.insert(key, value);
        }

        Ok(Message::StartupMessage(Self {
            protocol_version,
            parameters,
        }))
    }
}
