use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

/// Relation provide all information that we need to know to physically access a database relation.
#[derive(Clone)]
pub struct RelationData {
    /// Path where database files are stored.
    pub db_data: String,

    /// Name of database that this relation belongs.
    pub db_name: String,

    /// Name of this relation.
    pub rel_name: String,
}

/// A reference counter to an RelationData.
pub type Relation = Rc<RelationData>;

impl RelationData {
    /// Return the joined path of relation using the database and relation name.
    pub fn full_path(&self) -> PathBuf {
        Path::new(&self.db_data)
            .join(&self.db_name)
            .join(&self.rel_name)
    }
}
