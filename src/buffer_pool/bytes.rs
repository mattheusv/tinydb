/// Bytes is a wrapper over a byte array that makes it easy to write, overwrite and reset that byte array.
#[derive(PartialEq, Debug)]
pub struct Bytes<const N: usize> {
    data: [u8; N],
}

impl<const N: usize> Bytes<{ N }> {
    /// Create a new empty bytes buffer.
    pub fn new() -> Self {
        Self::from_bytes([0; N])
    }

    /// Create a new bytes buffer from a current array of bytes.
    pub fn from_bytes(data: [u8; N]) -> Self {
        Self { data }
    }

    /// Override the current bytes from buffer to the incoming data.
    pub fn write(&mut self, data: [u8; N]) {
        self.data = data;
    }

    /// Return the current bytes inside buffer.
    pub fn bytes(&self) -> [u8; N] {
        self.data
    }

    /// Return a mutable reference to override.
    pub fn bytes_mut(&mut self) -> &mut [u8; N] {
        &mut self.data
    }

    /// Resets the buffer to be empty, but it retains the underlying storage for use by future writes.
    pub fn reset(&mut self) {
        self.data = [0; N];
    }
}
