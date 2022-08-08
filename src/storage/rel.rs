use anyhow::Result;
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::Oid;

use super::smgr::{SMgrRelation, SMgrRelationData};

/// RelFileLocator provide all that we need to know to physically access a relation.
pub struct RelationLocatorData {
    /// Path where database files are stored.
    pub db_data: String,

    /// Name of database that this relation belongs.
    pub db_name: String,

    /// Oid of relation.
    pub oid: Oid,
}

pub type RelationLocator = Rc<RelationLocatorData>;

impl RelationLocatorData {
    /// Return the physical path of a relation.
    pub fn relation_path(&self) -> Result<PathBuf> {
        Ok(Path::new(&self.db_data)
            .join(&self.db_name)
            .join(&self.oid.to_string()))
    }
}

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
    pub fn open(oid: Oid, db_data: &str, db_name: &str, rel_name: &str) -> Relation {
        Rc::new(RefCell::new(RelationData {
            locator: Rc::new(RelationLocatorData {
                db_data: db_data.to_string(),
                db_name: db_name.to_string(),
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
