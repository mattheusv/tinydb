use crate::Oid;
use serde::{Deserialize, Serialize};

/// Fixed oid of pg_attribute relation.
pub const RELATION_OID: Oid = 1249;

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
}
