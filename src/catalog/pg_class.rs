use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    storage::rel::{Relation, RelationData},
    Oid,
};

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
    pub fn get_relation(db_data: &str, db_name: &str) -> Result<Relation> {
        Ok(RelationData::open(
            RELATION_OID,
            db_data,
            db_name,
            RELATION_NAME,
        )?)
    }
}
