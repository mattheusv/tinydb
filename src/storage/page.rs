use anyhow::Result;
use std::{
    io::{self, Seek},
    mem::size_of,
};

use serde::{Deserialize, Serialize};

use super::{Page, PageWriter, PAGE_SIZE};

/// Represents the fixed size of a page header.
pub const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();

const PG_PAGE_LAYOUT_VERSION: usize = 4;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct PageXLogRecPtr {
    /* high bits */
    pub xlogid: u32,

    /* low bits */
    pub xrecoff: u32,
}

/// Space management information generic to any page.
///
/// Note that the most fields of this is struct is set to 0 just to be
/// compatible with the Postgres page header.
#[derive(Serialize, Deserialize, Debug)]
pub struct PageHeader {
    /// LSN: next byte after last byte of WAL record for last change to this page
    pub pd_lsn: PageXLogRecPtr,

    /// Page checksum.
    pub pd_checksum: u16,

    /// Flag bits
    pub pd_flags: u16,

    /// Offset to start of free space
    pub pd_lower: u16,

    /// Offset to end of free space
    pub pd_upper: u16,

    /// Offset to start of special space
    pub pd_special: u16,

    /// Page size and layout version number information
    pub pd_pagesize_version: u16,

    /// Oldest unpruned XMAX on page, or zero if none
    pub pd_prune_xid: u32,
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
            pd_lower: PAGE_HEADER_SIZE as u16,
            pd_upper: PAGE_SIZE as u16,
            pd_lsn: PageXLogRecPtr::default(),
            pd_checksum: 0,
            pd_flags: 0,
            pd_special: PAGE_SIZE as u16,
            pd_pagesize_version: (PAGE_SIZE | PG_PAGE_LAYOUT_VERSION) as u16,
            pd_prune_xid: 0,
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
    let item_id_offset = header.pd_lower as usize;
    let item_id = ItemId {
        offset: header.pd_upper - item.len() as u16,
        length: item.len() as u16,
    };
    let mut page_writer = PageWriter::new(page);

    page_writer.seek(io::SeekFrom::Start(item_id_offset as u64))?;
    bincode::serialize_into(&mut page_writer, &item_id)?;

    // Write the new item on page.
    page_writer.write_at(item, io::SeekFrom::Start(item_id.offset as u64))?;

    // Adjust the page header
    header.pd_lower = (item_id_offset + size_of::<ItemId>()) as u16;
    header.pd_upper = item_id.offset;

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
            header.pd_lower, 28,
            "Expected start free space {}, got {}",
            header.pd_lower, 28
        );
        assert_eq!(
            header.pd_upper, 8188,
            "Expected end free space {}, got {}",
            header.pd_upper, 8188
        );

        Ok(())
    }

    #[test]
    fn test_default_page_header_values() {
        let header = PageHeader::default();

        assert!(header.pd_upper <= PAGE_SIZE as u16,);
        assert!(header.pd_upper <= header.pd_special,);
        assert!(header.pd_lower > (PAGE_HEADER_SIZE - ITEM_ID_SIZE) as u16);
        assert!(header.pd_lower < PAGE_SIZE as u16);
        assert!(header.pd_upper > header.pd_lower);
        assert!(header.pd_special <= PAGE_SIZE as u16);
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
