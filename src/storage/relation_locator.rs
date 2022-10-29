use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    catalog::pg_tablespace::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID},
    Oid, INVALID_OID,
};

pub type RelationLocator = Arc<RelationLocatorData>;

/// RelFileLocator provide all that we need to know to physically access a relation.
#[derive(Eq, PartialEq, Hash)]
pub struct RelationLocatorData {
    /// Tablespace oid where relation is stored.
    pub tablespace: Oid,

    /// Database oid that this relation belongs.
    pub database: Oid,

    /// Oid of relation.
    pub oid: Oid,
}

/// Return the physical path of a relation.
pub fn relation_path(tablespace: &Oid, db_oid: &Oid, rel_oid: &Oid) -> Result<PathBuf> {
    assert_ne!(*tablespace, INVALID_OID);
    assert_ne!(*rel_oid, INVALID_OID);

    match *tablespace {
        DEFAULTTABLESPACE_OID => {
            assert_ne!(*db_oid, INVALID_OID);
            Ok(Path::new("base")
                .join(&db_oid.to_string())
                .join(&rel_oid.to_string()))
        }
        GLOBALTABLESPACE_OID => {
            assert_ne!(*tablespace, INVALID_OID);
            Ok(Path::new("global").join(&rel_oid.to_string()))
        }
        _ => {
            todo!()
        }
    }
}
