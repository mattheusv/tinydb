use anyhow::Result;
use std::{
    io::{self, Seek},
    mem::size_of,
};

use serde::{Deserialize, Serialize};

use super::{Page, PageWriter, PAGE_SIZE};

/// Represents the fixed size of a page header.
pub const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();

/// Space management information generic to any page.
#[derive(Serialize, Deserialize, Debug)]
pub struct PageHeader {
    /// Offset to start of free space
    pub start_free_space: u16,

    /// Offset to end of free space
    pub end_free_space: u16,
}

impl PageHeader {
    /// Deserializa the page header for the given raw page data.
    pub fn new(page: &Page) -> Result<Self, bincode::Error> {
        let page = page.0.read().unwrap();
        bincode::deserialize::<PageHeader>(&page[0..PAGE_HEADER_SIZE])
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            start_free_space: PAGE_HEADER_SIZE as u16,
            end_free_space: PAGE_SIZE as u16,
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
pub fn page_add_item(page: &Page, item: &Vec<u8>) -> Result<()> {
    let mut header = PageHeader::new(page)?;

    // Select the offset number to place the new item
    let item_id_offset = header.start_free_space as usize;
    let item_id = ItemId {
        offset: header.end_free_space - item.len() as u16,
        length: item.len() as u16,
    };
    let mut page_writer = PageWriter::new(page);

    page_writer.seek(io::SeekFrom::Start(item_id_offset as u64))?;
    bincode::serialize_into(&mut page_writer, &item_id)?;

    // Write the new item on page.
    page_writer.write_at(item, io::SeekFrom::Start(item_id.offset as u64))?;

    // Adjust the page header
    header.start_free_space = (item_id_offset + size_of::<ItemId>()) as u16;
    header.end_free_space = item_id.offset - 1;

    // Write the adjusted page header at the in-memory page.
    page_writer.seek(io::SeekFrom::Start(0))?;
    bincode::serialize_into(&mut page_writer, &header)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_add_item() -> Result<(), bincode::Error> {
        // Create a new empty page with the default header values.
        let page = Page::default();

        let mut page_writer = PageWriter::new(&page);
        bincode::serialize_into(&mut page_writer, &PageHeader::default())?;

        let result = page_add_item(&page, &bincode::serialize(&150)?);
        assert!(
            result.is_ok(),
            "Failed to add new item on page: {}",
            result.err().unwrap()
        );

        let header = PageHeader::new(&page)?;
        assert_eq!(
            header.start_free_space, 8,
            "Expected start free space {}, got {}",
            header.start_free_space, 8
        );
        assert_eq!(
            header.end_free_space, 8187,
            "Expected end free space {}, got {}",
            8187, header.end_free_space,
        );

        Ok(())
    }

    #[test]
    fn test_default_page_header_values() {
        let header = PageHeader::default();

        assert!(header.end_free_space <= PAGE_SIZE as u16,);
        assert!(header.start_free_space > (PAGE_HEADER_SIZE - ITEM_ID_SIZE) as u16);
        assert!(header.start_free_space < PAGE_SIZE as u16);
        assert!(header.end_free_space > header.start_free_space);
    }

    #[test]
    fn test_item_id_size() {
        assert_eq!(ITEM_ID_SIZE, 4, "Item id size should have 4 bytes long");
    }

    #[test]
    fn test_page_header_size() {
        assert_eq!(
            PAGE_HEADER_SIZE, 4,
            "Page header size should have 4 bytes long"
        );
    }
}
