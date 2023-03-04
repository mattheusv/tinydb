pub mod relation;

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
            encode_i32(encode_to, &value.parse::<i32>()?)?;
        }
        ast::Value::SingleQuotedString(s) => {
            if attr.attlen >= 0 && (s.len() > attr.attlen as usize) {
                bail!("value too long for type character varying({})", attr.attlen);
            }
            encode_string(encode_to, &s)?;
        }
        ast::Value::Null => {
            encode_to.push(None);
        }
        ast::Value::Boolean(value) => {
            encode_boolean(encode_to, &value)?;
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
        pg_type::INT_OID => Ok(decode_i32(datum)?.to_string()),
        pg_type::VARCHAR_OID => decode_string(datum),
        pg_type::BOOL_OID => Ok(decode_boolean(datum)?.to_string()),
        _ => bail!("decode: Unsupported type to decode"),
    }
}
/// Encode a i32 value into a list of datum.
pub fn encode_i32(encode_to: &mut Datums, v: &i32) -> Result<()> {
    bincode::serialize_into(encode_to, &v)?;
    Ok(())
}

/// Decode the given datum as a i32.
pub fn decode_i32(datum: &Datum) -> Result<i32> {
    Ok(bincode::deserialize::<i32>(&datum)?)
}

/// Encode a bool value into a list of datum.
pub fn encode_boolean(encode_to: &mut Datums, v: &bool) -> Result<()> {
    bincode::serialize_into(encode_to, &v)?;
    Ok(())
}

/// Decode the given datum as a boolean.
pub fn decode_boolean(datum: &Datum) -> Result<bool> {
    Ok(bincode::deserialize::<bool>(&datum)?)
}

/// Encode a string value into a list of datum.
pub fn encode_string(encode_to: &mut Datums, v: &String) -> Result<()> {
    let varlena = Varlena::try_from(v)?;

    let data = bincode::serialize(&varlena)?;
    encode_to.push(Some(data));
    Ok(())
}

/// Decode the given datum as a string.
pub fn decode_string(datum: &Datum) -> Result<String> {
    Ok(bincode::deserialize::<String>(&datum)?)
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
