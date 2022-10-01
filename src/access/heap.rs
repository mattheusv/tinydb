use std::io::{Cursor, Read};
use std::{cell::RefCell, rc::Rc};

use crate::storage::buffer::Buffer;
use crate::{
    relation::Relation,
    storage::{
        freespace,
        page::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
        BufferPool,
    },
};
use anyhow::Result;

use super::heaptuple::HeapTuple;

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let mut page = buffer_pool.get_page(&buffer)?;

    page_add_item(&mut page, &tuple.encode()?)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(buffer_pool: Rc<RefCell<BufferPool>>, rel: &Relation) -> Result<Vec<HeapTuple>> {
    let mut tuples = Vec::new();
    let heap = HeapIterator::new(buffer_pool, rel)?;
    for tuple in heap {
        tuples.push(tuple?);
    }
    Ok(tuples)
}

/// Heap tuple iterator iterate over all heap tuples of a given relation.
///
/// HeapTupleIterator implements the Iterator trait.
pub struct HeapIterator {
    /// Buffer pool used to fetch buffers and get buffer page contents.
    buffer_pool: Rc<RefCell<BufferPool>>,

    /// Cursor used to read item id pointers.
    item_id_data_cursor: Cursor<Vec<u8>>,

    /// Holds the raw binary data used to deserialize a item
    /// id object.
    item_id_data: Vec<u8>,

    /// Current buffer used to scan. None if there is no more
    /// buffer to scan on page.
    buffer: Option<Buffer>,
}

impl Iterator for HeapIterator {
    type Item = Result<HeapTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next() {
            Ok(tuple) => match tuple {
                Some(tuple) => Some(Ok(tuple)),
                None => None,
            },
            Err(err) => Some(Err(err)),
        }
    }
}

impl HeapIterator {
    /// Create a new heap tuple iterator over the given relation.
    pub fn new(buffer_pool: Rc<RefCell<BufferPool>>, rel: &Relation) -> Result<Self> {
        // TODO: Iterate over all pages on relation
        let buffer = buffer_pool.borrow_mut().fetch_buffer(rel, 1)?;

        let page = buffer_pool.borrow().get_page(&buffer)?;
        let page_header = PageHeader::new(&page)?;

        let item_id_data = page.slice(PAGE_HEADER_SIZE, page_header.start_free_space as usize);

        Ok(Self {
            buffer_pool,
            buffer: Some(buffer),
            item_id_data: vec![0; ITEM_ID_SIZE],
            item_id_data_cursor: Cursor::new(item_id_data.to_vec()),
        })
    }

    /// Return the next tuple from buffer if exists. If the all tuples was readed
    /// from current buffer, try_next will check if there is more buffer's to
    /// be readed, if not, return None.
    fn try_next(&mut self) -> Result<Option<HeapTuple>> {
        match self.buffer {
            Some(buffer) => {
                let size = self.item_id_data_cursor.read(&mut self.item_id_data)?;
                if size == 0 {
                    // All item data pointers was readed, unpin the buffer
                    // and return None.
                    //
                    // TODO: Check if there is more buffers to read.
                    self.buffer_pool
                        .borrow_mut()
                        .unpin_buffer(buffer, false /* is_dirty*/)?;
                    return Ok(None);
                }

                let page = self.buffer_pool.borrow().get_page(&buffer)?;

                // Deserialize a single ItemId from the list item_id_data.
                let item_id = bincode::deserialize::<ItemId>(&self.item_id_data)?;

                // Slice the raw page to get a refenrece to a tuple inside the page.
                let data = &page.slice(
                    item_id.offset as usize,
                    (item_id.offset + item_id.length) as usize,
                );
                let tuple = HeapTuple::decode(data)?;

                self.item_id_data = vec![0; ITEM_ID_SIZE];

                Ok(Some(tuple))
            }
            // There is no more buffer's to scan.
            None => Ok(None),
        }
    }
}
