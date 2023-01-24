use std::io::{Cursor, Read};

use crate::storage;
use crate::storage::buffer::Buffer;
use crate::{
    relation::Relation,
    storage::{
        freespace,
        page::{page_add_item, ItemId, ITEM_ID_SIZE},
        BufferPool,
    },
};
use anyhow::Result;

use super::heaptuple::HeapTuple;

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer)?;

    page_add_item(&page, &tuple.encode()?)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

/// Heap tuple iterator iterate over all heap tuples of a given relation.
///
/// HeapTupleIterator implements the Iterator trait.
pub struct HeapScanner {
    /// Buffer pool used to fetch buffers and get buffer page contents.
    buffer_pool: BufferPool,

    /// Cursor used to read item id pointers.
    item_id_data_cursor: Cursor<Vec<u8>>,

    /// Holds the raw binary data used to deserialize a item
    /// id object.
    item_id_data: Vec<u8>,

    /// Current buffer used to scan. None if there is no more
    /// buffer to scan on page.
    buffer: Option<Buffer>,
}

impl HeapScanner {
    /// Create a new heap tuple iterator over the given relation.
    pub fn new(buffer_pool: BufferPool, rel: &Relation) -> Result<Self> {
        // TODO: Iterate over all pages on relation
        let buffer = buffer_pool.fetch_buffer(rel, 1)?;

        let page = buffer_pool.get_page(&buffer)?;
        let item_id_data = storage::item_id_data_from_page(&page)?;

        Ok(Self {
            buffer_pool,
            buffer: Some(buffer),
            item_id_data: vec![0; ITEM_ID_SIZE],
            item_id_data_cursor: Cursor::new(item_id_data.to_vec()),
        })
    }

    /// Return the next tuple from buffer if exists. If the all tuples was readed
    /// from current buffer, next_tuple will check if there is more buffer's to
    /// be readed, if not, return None.
    pub fn next_tuple(&mut self) -> Result<Option<HeapTuple>> {
        match self.buffer {
            Some(buffer) => {
                let size = self.item_id_data_cursor.read(&mut self.item_id_data)?;
                if size == 0 {
                    // All item data pointers was readed, unpin the buffer
                    // and return None.
                    //
                    // TODO: Check if there is more buffers to read.
                    self.buffer_pool.unpin_buffer(buffer, false /* is_dirty*/)?;
                    return Ok(None);
                }

                let page = self.buffer_pool.get_page(&buffer)?;

                // Deserialize a single ItemId from the list item_id_data.
                let item_id = bincode::deserialize::<ItemId>(&self.item_id_data)?;

                // Slice the raw page to get a refenrece to a tuple inside the page.
                let data = storage::value_from_page_item(&page, &item_id)?;
                let tuple = HeapTuple::decode(&data)?;

                self.item_id_data = vec![0; ITEM_ID_SIZE];

                Ok(Some(tuple))
            }
            // There is no more buffer's to scan.
            None => Ok(None),
        }
    }
}
