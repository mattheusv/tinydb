use crate::Oid;

// List of Oids for each type that tinydb supports
// Copy and pasted from src/backend/catalog/pg_type_d.h

pub const INT4_OID: Oid = 23;
pub const VARCHAR_OID: Oid = 1043;
pub const BOOL_OID: Oid = 16;
