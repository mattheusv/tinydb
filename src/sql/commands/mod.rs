pub mod create;
pub mod explain;
pub mod insert;
pub mod query;

/// Errors related with a SQL command
#[derive(Debug, thiserror::Error)]
pub enum SQLError {
    /// Unsupported SQL operation.
    #[error("unsuported operation {0}")]
    Unsupported(String),
}
