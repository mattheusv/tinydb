use anyhow::Result;
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{
    catalog::pg_tablespace::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID},
    Oid, INVALID_OID,
};

use super::smgr::{SMgrRelation, SMgrRelationData};

/// RelFileLocator provide all that we need to know to physically access a relation.
pub struct RelationLocatorData {
    /// Path where database files are stored.
    pub db_data: String,

    /// Tablespace oid where relation is stored.
    pub tablespace: Oid,

    /// Database oid that this relation belongs.
    pub database: Oid,

    /// Oid of relation.
    pub oid: Oid,
}

pub type RelationLocator = Rc<RelationLocatorData>;

impl RelationLocatorData {
    /// Return the physical path of a relation.
    pub fn relation_path(&self) -> Result<PathBuf> {
        assert_ne!(self.tablespace, INVALID_OID);
        assert_ne!(self.oid, INVALID_OID);

        let path = Path::new(&self.db_data);

        match self.tablespace {
            DEFAULTTABLESPACE_OID => {
                assert_ne!(self.database, INVALID_OID);
                Ok(path
                    .join("base")
                    .join(&self.database.to_string())
                    .join(&self.oid.to_string()))
            }
            GLOBALTABLESPACE_OID => {
                assert_ne!(self.tablespace, INVALID_OID);
                Ok(path.join("global").join(&self.oid.to_string()))
            }
            _ => {
                todo!()
            }
        }
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
