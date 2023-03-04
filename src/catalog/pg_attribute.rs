use crate::{access::heaptuple::TupleDesc, Oid};

use super::pg_type;

/// Fixed oid of pg_attribute relation.
pub const RELATION_OID: Oid = 1249;

pub const RELATION_NAME: &'static str = "pg_attribute";

/// The catalog pg_attribute stores information about table columns. There will be exactly one pg_attribute row for
/// every column in every table in the database.
#[derive(Debug, Clone)]
pub struct PgAttribute {
    /// The relation this column belongs to.
    pub attrelid: Oid,

    /// The column name.
    pub attname: String,

    /// The number of the column (start at 1).
    pub attnum: usize,

    /// The number of bytes in the internal representation of the type.
    pub attlen: i64,

    /// The data type of this column
    pub atttypid: Oid,
}

impl PgAttribute {
    /// Return the tuple description from pg_attribute system relation.
    pub fn tuple_desc() -> TupleDesc {
        TupleDesc {
            attrs: vec![
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attrelid"),
                    attnum: 1,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attname"),
                    attnum: 2,
                    attlen: -1,
                    atttypid: pg_type::VARCHAR_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attnum"),
                    attnum: 3,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
                PgAttribute {
                    attrelid: RELATION_OID,
                    attname: String::from("attlen"),
                    attnum: 4,
                    attlen: 8,
                    atttypid: pg_type::INT_OID,
                },
            ],
        }
    }
}
