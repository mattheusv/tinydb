use anyhow::{bail, Result};
use sqlparser::ast;

use crate::{errors::Error, Datum, Datums};

/// Encode the ast value to a Datum representation.
pub fn encode(encode_to: &mut Datums, value: &ast::Value) -> Result<()> {
    match value {
        ast::Value::Number(value, _) => {
            let value = value.parse::<i32>()?;
            bincode::serialize_into(encode_to, &value)?;
        }
        ast::Value::Null => {
            encode_to.push(None);
        }
        _ => bail!(Error::UnsupportedValue(value.to_string())),
    };
    Ok(())
}

/// Decode a raw tuple to a SQL value.
//
// TODO: Change the return type to a more generic type
// that represents a SQL value.
//
// TODO: Add support for more data types.
pub fn decode(datum: &Datum) -> Result<String> {
    let value = bincode::deserialize::<i32>(&datum)?;
    Ok(value.to_string())
}
