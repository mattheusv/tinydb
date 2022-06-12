use anyhow::{bail, Result};
use std::path::Path;

use crate::{
    access::heap::heap_iter,
    new_object_id,
    storage::{rel::RelationData, BufferPool},
    Oid,
};

use self::pg_class::PgClass;

pub mod heap;
pub mod pg_class;

/// Genereate a new relation oid that is unique within the database of the given db data.
pub fn new_relation_oid(db_data: &str, db_name: &str) -> Oid {
    let dbpath = Path::new(db_data).join(db_name);

    loop {
        let oid = new_object_id();
        if !dbpath.join(oid.to_string()).exists() {
            return oid;
        }
    }
}

/// Errors related with system catalog relation operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("relation {0} does not exist")]
    RelationNotFound(String),
}

/// Struct catalog hold rountines and utilities to deal with system catalog relations.
pub struct Catalog {
    /// Base data directoy.
    db_data: String,
}

impl Catalog {
    /// Create a new catalog instance using db_data as base data directoy.
    pub fn new(db_data: &str) -> Self {
        Self {
            db_data: db_data.to_string(),
        }
    }

    /// Return the oid of the given relation name.
    pub fn get_oid_relation(
        &self,
        buffer_pool: &mut BufferPool,
        db_name: &str,
        rel_name: &str,
    ) -> Result<Oid> {
        // TODO: The catalog relations should also be stored inside pg_class.
        match rel_name {
            "pg_class" => Ok(pg_class::RELATION_OID),
            _ => {
                let pg_class_rel =
                    RelationData::open(pg_class::RELATION_OID, &self.db_data, db_name, "pg_class")?;

                let mut oid = None;

                heap_iter(buffer_pool, &pg_class_rel, |tuple| -> Result<()> {
                    // Do nothing if the oid is already founded.
                    if oid.is_none() {
                        let pg_class = bincode::deserialize::<PgClass>(&tuple)?;
                        if pg_class.relname == rel_name {
                            oid = Some(pg_class.oid);
                        }
                    }
                    Ok(())
                })?;

                match oid {
                    Some(oid) => Ok(oid),
                    None => bail!(Error::RelationNotFound(rel_name.to_string())),
                }
            }
        }
    }
}
