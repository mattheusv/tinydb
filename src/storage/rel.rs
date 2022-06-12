use anyhow::Result;
use std::{cell::RefCell, path::Path, rc::Rc};

use crate::Oid;

use super::pager::Pager;

/// Relation provide all information that we need to know to physically access a database relation.
pub struct RelationData {
    /// Oid of relation.
    pub oid: Oid,

    /// Path where database files are stored.
    pub db_data: String,

    /// Name of database that this relation belongs.
    pub db_name: String,

    /// Name of this relation.
    pub rel_name: String,

    /// File pager handle.
    pub pager: Pager,
}

/// A mutable reference counter to an RelationData.
pub type Relation = Rc<RefCell<RelationData>>;

impl RelationData {
    /// Open any relation to the given db data path and db name and relation name.
    pub fn open(oid: Oid, db_data: &str, db_name: &str, rel_name: &str) -> Result<Relation> {
        let pager = Pager::open(&Path::new(db_data).join(db_name).join(oid.to_string()))?;
        Ok(Rc::new(RefCell::new(RelationData {
            oid,
            pager,
            db_data: db_data.to_string(),
            db_name: db_name.to_string(),
            rel_name: rel_name.to_string(),
        })))
    }
}
