use std::convert::TryFrom;

use anyhow::{bail, Result};
use sqlparser::ast;

use crate::{
    access::heaptuple::Varlena,
    catalog::{pg_attribute::PgAttribute, pg_type},
    errors::Error,
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
        _ => bail!(Error::UnsupportedValue(value.to_string())),
    };
    Ok(())
}

/// Decode a raw tuple to a SQL value.
//
// TODO: Change the return type to a more generic type
// that represents a SQL value.
pub fn decode(datum: &Datum, typ: Oid) -> Result<String> {
    match typ {
        pg_type::INT4_OID => Ok(bincode::deserialize::<i32>(&datum)?.to_string()),
        pg_type::VARCHAR_OID => Ok(bincode::deserialize::<String>(&datum)?),
        _ => bail!("decode: Unsupported type to decode"),
    }
}
