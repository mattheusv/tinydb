use serde::{Deserialize, Serialize};

use crate::Oid;

/// Fixed oid of pg_class relation.
pub const RELATION_OID: Oid = 1259;

/// The catalog pg_class catalogs tables and most everything else that has columns or is otherwise similar to a table.
#[derive(Serialize, Deserialize, Debug)]
pub struct PgClass {
    /// OID of relation.
    pub oid: Oid,

    /// Relation name.
    pub relname: String,
}
