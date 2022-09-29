use std::mem::size_of;

use serde::{Deserialize, Serialize};

use super::{buffer::MemPage, PAGE_SIZE};

/// Represents the fixed size of a page header.
pub const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();

/// Space management information generic to any page.
#[derive(Serialize, Deserialize, Debug)]
pub struct PageHeader {
    /// Offset to start of free space
    pub start_free_space: u16,

    /// Offset to end of free space
    pub end_free_space: u16,

    _padding: [u8; 20],
}

impl PageHeader {
    /// Deserializa the page header for the given raw page data.
    pub fn new(page: &MemPage) -> Result<Self, bincode::Error> {
        bincode::deserialize::<PageHeader>(&page.borrow().bytes()[0..PAGE_HEADER_SIZE])
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            start_free_space: PAGE_HEADER_SIZE as u16,
            end_free_space: PAGE_SIZE as u16,
            _padding: [0; 20],
        }
    }
}

/// Offset number of an item on buffer page.
pub type OffsetNumber = u16;

/// A line pointer on a buffer page.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ItemId {
    /// Offset to tuple (from start of page)
    pub offset: OffsetNumber,

    /// Byte length of tuple.
    pub length: u16,
}

/// Size of an item id on heap page.
pub const ITEM_ID_SIZE: usize = size_of::<ItemId>();

/// Add a new item to a page. The page header start_free_space and end_free_space is also updated
/// to point to the new offsets after the item is inserted on in-memory page.
pub fn page_add_item(page: &MemPage, item: &Vec<u8>) -> Result<(), bincode::Error> {
    let mut header = PageHeader::new(page)?;
    let mut page = page.borrow_mut();

    // Select the offset number to place the new item
    let item_id_offset = header.start_free_space as usize;
    let item_id = ItemId {
        offset: header.end_free_space - item.len() as u16,
        length: item.len() as u16,
    };
    page.write_at(&bincode::serialize(&item_id)?, item_id_offset);

    // Write the new item on page.
    page.write_at(item, item_id.offset as usize);

    // Adjust the page header
    header.start_free_space = (item_id_offset + size_of::<ItemId>()) as u16;
    header.end_free_space = item_id.offset;

    // Write the adjusted page header at the in-memory page.
    page.write_at(&bincode::serialize(&header)?, 0);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::storage::buffer::Bytes;

    use super::*;

    #[test]
    fn test_page_add_item() -> Result<(), bincode::Error> {
        // Create a new empty page with the default header values.
        let page = Rc::new(RefCell::new(Bytes::new()));
        page.borrow_mut()
            .write_at(&bincode::serialize(&PageHeader::default())?, 0);

        let result = page_add_item(&page, &bincode::serialize(&150)?);
        assert!(
            result.is_ok(),
            "Failed to add new item on page: {}",
            result.err().unwrap()
        );

        let header = PageHeader::new(&page)?;
        assert_eq!(header.start_free_space, 28);
        assert_eq!(header.end_free_space, 8188);

        Ok(())
    }

    #[test]
    fn test_item_id_size() {
        assert_eq!(ITEM_ID_SIZE, 4, "Item id size should have 4 bytes long");
    }

    #[test]
    fn test_page_header_size() {
        assert_eq!(
            PAGE_HEADER_SIZE, 24,
            "Page header size should have 24 bytes long"
        );
    }
}
