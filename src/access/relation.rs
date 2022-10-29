use std::sync::Arc;

use crate::{
    catalog::{
        pg_attribute, pg_class, pg_database,
        pg_tablespace::{self, DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID},
    },
    relation::{Relation, RelationData},
    storage::relation_locator::RelationLocatorData,
    Oid, INVALID_OID,
};

/// Open any relation to the given db data path and db name and relation name.
pub fn open_relation(oid: Oid, tablespace: Oid, db_oid: &Oid, rel_name: &str) -> Relation {
    Arc::new(RelationData::new(
        Arc::new(RelationLocatorData {
            database: db_oid.clone(),
            tablespace,
            oid,
        }),
        rel_name,
    ))
}

/// Return the pg_class Relation.
pub fn open_pg_class_relation(db_oid: &Oid) -> Relation {
    open_relation(
        pg_class::RELATION_OID,
        DEFAULTTABLESPACE_OID,
        db_oid,
        pg_class::RELATION_NAME,
    )
}

/// Return the pg_attribute Relation.
pub fn open_pg_attribute_relation(db_oid: &Oid) -> Relation {
    open_relation(
        pg_attribute::RELATION_OID,
        DEFAULTTABLESPACE_OID,
        db_oid,
        pg_attribute::RELATION_NAME,
    )
}

/// Return the pg_database Relation.
pub fn open_pg_database_relation() -> Relation {
    open_relation(
        pg_database::RELATION_OID,
        GLOBALTABLESPACE_OID,
        &INVALID_OID,
        pg_database::RELATION_NAME,
    )
}

/// Return the pg_tablespace Relation.
pub fn open_pg_tablespace_relation() -> Relation {
    open_relation(
        pg_tablespace::RELATION_OID,
        GLOBALTABLESPACE_OID,
        &INVALID_OID,
        pg_tablespace::RELATION_NAME,
    )
}
