use serde::{Deserialize, Serialize};

use crate::{
    access::tuple::TupleDesc,
    relation::{Relation, RelationData},
    sql::encode::{varlena_deserializer, varlena_serializer},
    Oid, INVALID_OID,
};

use super::{pg_attribute::PgAttribute, pg_tablespace::GLOBALTABLESPACE_OID, pg_type};

/// Fixed oid of pg_class relation.
pub const RELATION_OID: Oid = 1262;

pub const RELATION_NAME: &'static str = "pg_database";

pub const TINYDB_OID: Oid = 5;

#[derive(Serialize, Deserialize, Debug)]
pub struct PgDatabase {
    /// Oid of database.
    pub oid: Oid,

    /// Database name.
    #[serde(deserialize_with = "varlena_deserializer")]
    #[serde(serialize_with = "varlena_serializer")]
    pub datname: String,

    /// The default tablespace for the database.
    pub dattablespace: Oid,
}

impl PgDatabase {
    /// Return the pg_database Relation.
    pub fn relation(db_data: &str) -> Relation {
        RelationData::open(
            RELATION_OID,
            db_data,
            GLOBALTABLESPACE_OID,
            &INVALID_OID,
            RELATION_NAME,
        )
    }

    /// Return the tuple description from pg_database system relation.
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
                    attname: String::from("datname"),
                    attnum: 2,
                    attlen: -1,
                    atttypid: pg_type::VARCHAR_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("dattablespace"),
                    attnum: 3,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
            ],
        }
    }
}
