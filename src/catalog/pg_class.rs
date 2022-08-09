use serde::{Deserialize, Serialize};

use crate::{
    access::tuple::TupleDesc,
    storage::rel::{Relation, RelationData},
    Oid,
};

use super::pg_attribute::PgAttribute;

/// Fixed oid of pg_class relation.
pub const RELATION_OID: Oid = 1259;

pub const RELATION_NAME: &'static str = "pg_class";

/// The catalog pg_class catalogs tables and most everything else that has columns or is otherwise similar to a table.
#[derive(Serialize, Deserialize, Debug)]
pub struct PgClass {
    /// OID of relation.
    pub oid: Oid,

    /// Relation name.
    pub relname: String,
}

impl PgClass {
    /// Return the pg_class Relation.
    pub fn relation(db_data: &str, db_name: &str) -> Relation {
        RelationData::open(RELATION_OID, db_data, db_name, RELATION_NAME)
    }

    /// Return the tuple description from pg_class system relation.
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
                    attname: String::from("relname"),
                    attnum: 2,
                    attlen: 7,
                },
            ],
        }
    }
}
