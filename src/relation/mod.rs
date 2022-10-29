use std::sync::Arc;

use crate::storage::{
    relation_locator::RelationLocator,
    smgr::{SMgrRelation, SMgrRelationData},
};

/// Relation provide all information that we need to know to physically access a database relation.
pub struct RelationData {
    /// Relation physical identifier.
    pub locator: RelationLocator,

    /// Name of this relation.
    pub rel_name: String,

    /// File entry handler of relation.
    pub smgr: SMgrRelation,
}

/// A mutable reference counter to an RelationData.
pub type Relation = Arc<RelationData>;

impl RelationData {
    pub fn new(locator: RelationLocator, rel_name: &str) -> Self {
        Self {
            locator: locator.clone(),
            rel_name: rel_name.to_string(),
            smgr: Arc::new(SMgrRelationData::new(&locator)),
        }
    }
}
