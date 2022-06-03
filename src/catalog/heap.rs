use crate::storage::{buffer, bufpage::PageHeader, pager::PAGE_SIZE, rel::Relation, BufferPool};

/// Create and initialize a new heap file relation to the given relation.
pub fn heap_create(buffer: &mut BufferPool, rel: &Relation) -> Result<(), buffer::Error> {
    let buf_id = buffer.alloc_buffer(rel)?;

    let mut data = bincode::serialize(&PageHeader::default()).unwrap();
    data.resize(PAGE_SIZE, u8::default());

    let page = buffer.get_page(&buf_id);
    page.borrow_mut().write_from_vec(data);

    buffer.unpin_buffer(buf_id, true)?;
    Ok(())
}
