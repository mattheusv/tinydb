use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{
    catalog::pg_tablespace::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID},
    Oid, INVALID_OID,
};

pub type RelationLocator = Rc<RelationLocatorData>;

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
