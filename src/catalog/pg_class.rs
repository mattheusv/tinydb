use serde::{Deserialize, Serialize};

use crate::Oid;

/// The catalog pg_class catalogs tables and most everything else that has columns or is otherwise similar to a table.
#[derive(Serialize, Deserialize)]
pub struct PgClass {
    /// Relation name.
    pub relname: String,
}
