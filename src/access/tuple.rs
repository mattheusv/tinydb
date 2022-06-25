use crate::catalog::pg_attribute::PgAttribute;

/// Describe the structure of tuples. Basically it holds the columns of tables.
pub struct TupleDesc {
    /// Columns of table.
    pub attrs: Vec<PgAttribute>,
}

impl Default for TupleDesc {
    fn default() -> Self {
        Self { attrs: Vec::new() }
    }
}
