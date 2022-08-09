use crate::{
    access::tuple::TupleDesc,
    storage::rel::{Relation, RelationData},
    Oid,
};
use serde::{Deserialize, Serialize};

/// Fixed oid of pg_attribute relation.
pub const RELATION_OID: Oid = 1249;

pub const RELATION_NAME: &'static str = "pg_attribute";

/// The catalog pg_attribute stores information about table columns. There will be exactly one pg_attribute row for
/// every column in every table in the database.
#[derive(Serialize, Deserialize, Debug)]
pub struct PgAttribute {
    /// The relation this column belongs to.
    pub attrelid: Oid,

    /// The column name.
    pub attname: String,

    /// The number of the column.
    pub attnum: usize,

    /// The number of bytes in the internal representation of the type.
    pub attlen: usize,
}

impl PgAttribute {
    /// Return the pg_attribute Relation.
    pub fn relation(db_data: &str, db_name: &str) -> Relation {
        RelationData::open(RELATION_OID, db_data, db_name, RELATION_NAME)
    }

    /// Return the tuple description from pg_attribute system relation.
    pub fn tuple_desc() -> TupleDesc {
        TupleDesc {
            attrs: vec![
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attrelid"),
                    attnum: 1,
                    attlen: 8,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attname"),
                    attnum: 2,
                    attlen: 7,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attnum"),
                    attnum: 3,
                    attlen: 6,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attlen"),
                    attnum: 4,
                    attlen: 6,
                },
            ],
        }
    }
}
