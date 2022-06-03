use std::rc::Rc;

use crate::storage::rel::{Relation, RelationData};

pub fn new_relation(db_data: &str, db_name: &str, rel_name: &str) -> Relation {
    Rc::new(RelationData {
        db_data: db_data.to_string(),
        db_name: db_name.to_string(),
        rel_name: rel_name.to_string(),
    })
}
