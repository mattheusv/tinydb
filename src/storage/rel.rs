use std::path::{Path, PathBuf};

/// Relation provide all information that we need to know to physically access a database relation.
#[derive(Clone)]
pub struct Relation {
    /// Name of database that this relation belongs.
    pub db_name: String,

    /// Name of this relation.
    pub rel_name: String,
}

impl Relation {
    /// Return the joined path of relation using the database and relation name.
    pub fn full_path(&self) -> PathBuf {
        Path::new(&self.db_name).join(&self.rel_name)
    }
}
