pub mod buffer;
pub mod disk;
pub mod freespace;
pub mod page;
pub mod relation_locator;
pub mod smgr;

use std::{
    io::{self, Cursor, Read, Seek, Write},
    sync::{Arc, RwLock},
};

use self::page::{ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE};

pub use buffer::BufferPool;

/// Pages are numbered sequentially, starting at 0.
pub type PageNumber = u32;

/// An invalid page number that doest not exists on disk.
///
/// It can be used by buffer pool to indicates that a slot
/// that holds a page pointer is available to be used to store
/// a page inside.
pub const INVALID_PAGE_NUMBER: PageNumber = 0;

/// The size of a Page on database file.
pub const PAGE_SIZE: usize = 8192;

/// Each data file (heap or index) is divided into disk blocks, (which may be
/// thought of as the unit of IO). A Page contains exactly one disk block.
///
/// Page represents a mutable reference counter to a disk block. Page is
/// reference counted and clonning will just increase the reference counter.
///
/// A page is a read only, to write data on buffer page use the PageWriter
/// object, that will create a new page writer, writing incomming buffer data in
/// a mutable shared reference of a page.
///
/// The storage manager is the only pieces of code that should be accessing disk
/// blocks directly.
#[derive(Debug)]
pub struct Page(Arc<RwLock<[u8; PAGE_SIZE]>>);

impl Page {
    pub fn new(page: [u8; PAGE_SIZE]) -> Self {
        Self(Arc::new(RwLock::new(page)))
    }
}

impl Default for Page {
    fn default() -> Self {
        Self(Arc::new(RwLock::new([0; PAGE_SIZE])))
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.0.read().unwrap().as_ref() == other.0.read().unwrap().as_ref()
    }
}

/// Return the array of item identifiers pointing to the actual items.
pub fn item_id_data_from_page(page: &Page) -> anyhow::Result<Vec<ItemId>> {
    let page_header = PageHeader::new(page)?;

    let page = page.0.read().unwrap();
    let line_pointers_data = &page[PAGE_HEADER_SIZE..(page_header.start_free_space) as usize];

    // Pre allocate the line pointers array.
    let mut line_pointers = Vec::with_capacity(line_pointers_data.len() / ITEM_ID_SIZE);

    // Reuse the same block of memory to parse the item id.
    let mut item_id = vec![0; ITEM_ID_SIZE];

    // Create a cursor to consume the line pointer binary array.
    let mut cursor = Cursor::new(line_pointers_data);
    loop {
        let size = cursor.read(&mut item_id)?;
        if size == 0 {
            break;
        }

        let item_id = bincode::deserialize(&item_id)?;
        line_pointers.push(item_id);
    }

    Ok(line_pointers)
}

/// Return the raw value of the given item inside the given page.
pub fn value_from_page_item(page: &Page, item: &ItemId) -> anyhow::Result<Vec<u8>> {
    let page = page.0.read().unwrap();

    Ok(page[item.offset as usize..(item.offset + item.length) as usize].to_vec())
}

/// A buffer page writer.
///
/// BufferPageWriter implements std::io::Write and std::io::Seek traits
/// so it can be used as a writer parameter when serializing data.
pub struct PageWriter {
    /// Current position of writer to write incommig buffer data.
    pos: usize,

    /// Mutable shared reference to write incomming data.
    page: Page,
}

impl io::Write for PageWriter {
    /// Write the incomming buf on in memory referente of page.
    ///
    /// The incomming buf lenght can not exceed the PAGE_SIZE.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut page = self.page.0.write().unwrap();

        let new_size = self.pos + buf.len();
        if new_size > page.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Size of buffer {} can not be greater than {}",
                    new_size,
                    page.len(),
                ),
            ));
        }

        let mut current_pos = self.pos;
        for b in buf {
            page[current_pos] = b.clone();
            current_pos += 1;
        }

        self.pos = current_pos;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PageWriter {
    /// Create a new page write, writing data on the given page.
    pub fn new(page: &Page) -> Self {
        Self {
            pos: 0,
            page: page.clone(),
        }
    }

    /// An wrapper around seek and write calls.
    ///
    /// Start to write the incomming buf data that the given offset.
    pub fn write_at(&mut self, buf: &[u8], offset: io::SeekFrom) -> anyhow::Result<usize> {
        self.seek(offset)?;
        let size = self.write(buf)?;
        Ok(size)
    }
}

impl io::Seek for PageWriter {
    /// Change the current position of buffer page writer.
    fn seek(&mut self, pos: io::SeekFrom) -> std::io::Result<u64> {
        let page = self.page.0.read().unwrap();

        let page_size = page.len();
        match pos {
            std::io::SeekFrom::Start(pos) => {
                self.pos = pos as usize;
            }
            std::io::SeekFrom::End(pos) => {
                self.pos = page_size + pos as usize;
            }
            std::io::SeekFrom::Current(pos) => {
                self.pos += pos as usize;
            }
        };

        if self.pos >= page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Can not seek for a position {} that is greater than page size {}.",
                    self.pos, page_size,
                ),
            ));
        }

        Ok(self.pos as u64)
    }
}
