/// Generic errors that tinydb can generate by a due to a user input.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unsupported value type.
    #[error("unsuported value {0}")]
    UnsupportedValue(String),

    /// Unsupported SQL operation.
    #[error("unsuported operation {0}")]
    UnsupportedOperation(String),
}
