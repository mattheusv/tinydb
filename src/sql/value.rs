use std::convert::TryFrom;

use anyhow::{bail, Result};
use sqlparser::ast;

use crate::catalog::pg_attribute::PgAttribute;

use super::encode::Varlena;

pub enum Value {
    Int32(i32),

    Boolean(bool),

    Varlena(Varlena),

    Null,
}

impl Value {
    pub fn new(value: &sqlparser::ast::Value, attr: &PgAttribute) -> Result<Self> {
        match value {
            ast::Value::Number(value, _) => {
                let value = value.parse::<i32>()?;
                Ok(Self::Int32(value))
            }
            ast::Value::SingleQuotedString(s) => {
                if attr.attlen >= 0 && (s.len() > attr.attlen as usize) {
                    bail!("value too long for type character varying({})", attr.attlen);
                }
                let varlena = Varlena::try_from(s)?;

                Ok(Self::Varlena(varlena))
            }
            ast::Value::Null => Ok(Self::Null),
            ast::Value::Boolean(value) => Ok(Self::Boolean(value.to_owned())),
            _ => bail!("Unsupported value {}", value.to_string()),
        }
    }
}
