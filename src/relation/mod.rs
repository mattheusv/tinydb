use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

use crate::{
    storage::smgr::{SMgrRelation, SMgrRelationData},
    Oid,
};

use self::locator::{RelationLocator, RelationLocatorData};

pub mod locator;

/// Relation provide all information that we need to know to physically access a database relation.
pub struct RelationData {
    /// Relation physical identifier.
    pub locator: RelationLocator,

    /// Name of this relation.
    pub rel_name: String,

    /// Cache file handle or None if was not required yet.
    smgr: Option<SMgrRelation>,
}

/// A mutable reference counter to an RelationData.
pub type Relation = Rc<RefCell<RelationData>>;

impl RelationData {
    /// Open any relation to the given db data path and db name and relation name.
    pub fn open(
        oid: Oid,
        db_data: &str,
        tablespace: Oid,
        db_oid: &Oid,
        rel_name: &str,
    ) -> Relation {
        Rc::new(RefCell::new(RelationData {
            locator: Rc::new(RelationLocatorData {
                db_data: db_data.to_string(),
                database: db_oid.clone(),
                tablespace,
                oid,
            }),
            rel_name: rel_name.to_string(),
            smgr: None,
        }))
    }

    /// Returns smgr file handle for a relation, opening it if needed.
    pub fn smgr(&mut self) -> Result<SMgrRelation> {
        match &self.smgr {
            Some(smgr) => {
                return Ok(smgr.clone());
            }
            None => {
                let smgr = SMgrRelationData::open(&self.locator)?;
                self.smgr = Some(Rc::new(RefCell::new(smgr)));
                return self.smgr();
            }
        }
    }
}
