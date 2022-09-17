use serde::{Deserialize, Serialize};

use crate::{
    access::tuple::TupleDesc,
    relation::{Relation, RelationData},
    sql::encode::{varlena_deserializer, varlena_serializer},
    Oid,
};

use super::{pg_attribute::PgAttribute, pg_tablespace::DEFAULTTABLESPACE_OID, pg_type};

/// Fixed oid of pg_class relation.
pub const RELATION_OID: Oid = 1259;

pub const RELATION_NAME: &'static str = "pg_class";

/// The catalog pg_class catalogs tables and most everything else that has columns or is otherwise similar to a table.
#[derive(Serialize, Deserialize, Debug)]
pub struct PgClass {
    /// OID of relation.
    pub oid: Oid,

    /// Relation name.
    #[serde(deserialize_with = "varlena_deserializer")]
    #[serde(serialize_with = "varlena_serializer")]
    pub relname: String,

    /// The tablespace in which this relation is stored.
    pub reltablespace: Oid,

    /// True if this table is shared across all databases in the cluster. Only certain system
    /// catalogs (such as pg_database) are shared.
    pub relisshared: bool,
}

impl PgClass {
    /// Return the pg_class Relation.
    pub fn relation(db_data: &str, db_oid: &Oid) -> Relation {
        RelationData::open(
            RELATION_OID,
            db_data,
            DEFAULTTABLESPACE_OID,
            db_oid,
            RELATION_NAME,
        )
    }

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
                    attname: String::from("relname"),
                    attnum: 2,
                    attlen: -1,
                    atttypid: pg_type::VARCHAR_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("reltablespace"),
                    attnum: 3,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("relisshared"),
                    attnum: 4,
                    attlen: 1,
                    atttypid: pg_type::BOOL_OID,
                },
            ],
        }
    }
}
