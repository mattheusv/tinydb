use std::{convert::TryFrom, mem::size_of};

use anyhow::{bail, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlparser::ast;

use crate::{
    catalog::{pg_attribute::PgAttribute, pg_type},
    Datum, Datums, Oid,
};

/// Encode the ast value to a Datum representation.
pub fn encode(encode_to: &mut Datums, value: &ast::Value, attr: &PgAttribute) -> Result<()> {
    match value {
        ast::Value::Number(value, _) => {
            let value = value.parse::<i32>()?;
            bincode::serialize_into(encode_to, &value)?;
        }
        ast::Value::SingleQuotedString(s) => {
            if attr.attlen >= 0 && (s.len() > attr.attlen as usize) {
                bail!("value too long for type character varying({})", attr.attlen);
            }
            let varlena = Varlena::try_from(s)?;

            // TODO: Try to understand why I can't use bincode::serialize_into(encode_to, &varlena)
            // here.
            // Seems that using serialize_into the datums that are appended is splited between
            // multiple vectors, which cause erros when reading.
            let data = bincode::serialize(&varlena)?;
            encode_to.push(Some(data));
        }
        ast::Value::Null => {
            encode_to.push(None);
        }
        ast::Value::Boolean(value) => {
            bincode::serialize_into(encode_to, value)?;
        }
        _ => bail!("Unsupported value {}", value.to_string()),
    };
    Ok(())
}

/// Decode a raw tuple to a SQL value.
//
// TODO: Change the return type to a more generic type
// that represents a SQL value.
pub fn decode(datum: &Datum, typ: Oid) -> Result<String> {
    match typ {
        pg_type::INT_OID => Ok(bincode::deserialize::<i32>(&datum)?.to_string()),
        pg_type::VARCHAR_OID => Ok(bincode::deserialize::<String>(&datum)?),
        pg_type::BOOL_OID => Ok(bincode::deserialize::<bool>(&datum)?.to_string()),
        _ => bail!("decode: Unsupported type to decode"),
    }
}

/// Variable-length datatypes all share the 'struct varlena' header.
#[derive(Debug, Serialize, Deserialize)]
pub struct Varlena {
    /// Total length of the value in bytes
    ///
    /// v_len originally does no include itself, call len()
    /// to get the total length of varlena value (v_len + v_data).
    pub v_len: u32,

    /// Data contents
    pub v_data: Vec<u8>,
}

impl TryFrom<&String> for Varlena {
    type Error = bincode::Error;

    /// Create a new varlena from a string.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let data = bincode::serialize(&value)?;
        Ok(Self {
            v_len: bincode::serialize(&data)?.len() as u32,
            v_data: data,
        })
    }
}

impl Varlena {
    /// Compute the total length of varlena value.
    pub fn len(&self) -> usize {
        size_of::<u32>() + self.v_len as usize
    }
}

/// Serialize a string value into varlena struct format with len and raw  bytes representation.
pub fn varlena_serializer<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let varlena = Varlena::try_from(value).unwrap();
    varlena.serialize(serializer)
}

/// Deserialize a varlena string value.
pub fn varlena_deserializer<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let varlena = Varlena::deserialize(deserializer)?;
    let value = bincode::deserialize(&varlena.v_data).unwrap();
    Ok(value)
}
