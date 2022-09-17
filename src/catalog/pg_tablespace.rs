use crate::{
    access::tuple::TupleDesc,
    sql::encode::{varlena_deserializer, varlena_serializer},
    Oid,
};

use serde::{Deserialize, Serialize};

use super::{pg_attribute::PgAttribute, pg_type};

/// Fixed oid of pg_attribute relation.
pub const RELATION_OID: Oid = 1213;

pub const RELATION_NAME: &'static str = "pg_tablespace";

/// Default tablespace oid to store per database relation files.
pub const DEFAULTTABLESPACE_OID: Oid = 1663;

/// Global tablespace oid to store global database relation files, such as pg_database and
/// pg_tablespace.
pub const GLOBALTABLESPACE_OID: Oid = 1664;

#[derive(Serialize, Deserialize, Debug)]
pub struct PgTablespace {
    /// OID of tablespace.
    pub oid: Oid,

    /// Tablespace name.
    #[serde(deserialize_with = "varlena_deserializer")]
    #[serde(serialize_with = "varlena_serializer")]
    pub spcname: String,
}

impl PgTablespace {
    /// Return the tuple description from pg_class system relation.
    pub fn tuple_desc() -> TupleDesc {
        TupleDesc {
            attrs: vec![
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("oid"),
                    attnum: 1,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("spcname"),
                    attnum: 2,
                    attlen: -1,
                    atttypid: pg_type::VARCHAR_OID,
                },
            ],
        }
    }
}
