use anyhow::Result;

use super::{buffer::Buffer, rel::Relation, BufferPool};

/// Try to find a page in the given relation with at least the specified amount of free space.
///
// TODO: Implement visibility map to find free page to add a new tuple
pub fn get_page_with_free_space(buffer: &mut BufferPool, rel: &Relation) -> Result<Buffer> {
    buffer.fetch_buffer(rel, 1)
}
