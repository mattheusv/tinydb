use serde::{Deserialize, Serialize};

use crate::{
    access::tuple::TupleDesc,
    storage::rel::{Relation, RelationData},
    Oid,
};

use super::pg_attribute::PgAttribute;

/// Fixed oid of pg_class relation.
pub const RELATION_OID: Oid = 1262;

pub const RELATION_NAME: &'static str = "pg_database";

#[derive(Serialize, Deserialize, Debug)]
pub struct PgDatabase {
    /// Oid of database.
    pub oid: Oid,

    /// Database name.
    pub datname: String,

    /// The default tablespace for the database.
    pub dattablespace: Oid,
}

impl PgDatabase {
    /// Return the pg_database Relation.
    pub fn relation(db_data: &str, db_name: &str) -> Relation {
        RelationData::open(RELATION_OID, db_data, db_name, RELATION_NAME)
    }

    /// Return the tuple description from pg_database system relation.
    pub fn tuple_desc() -> TupleDesc {
        TupleDesc {
            attrs: vec![
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("oid"),
                    attnum: 1,
                    attlen: 3,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("datname"),
                    attnum: 2,
                    attlen: 7,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("dattablespace"),
                    attnum: 3,
                    attlen: 13,
                },
            ],
        }
    }
}
