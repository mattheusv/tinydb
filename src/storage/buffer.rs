use crate::lru::LRU;
use crate::storage::{pager, pager::PageNumber, pager::Pager, pager::PAGE_SIZE};
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::rc::Rc;

use super::rel::Relation;

/// Represents errors that buffer pool can have.
#[derive(Debug)]
pub enum Error {
    /// Represents no free slots on buffer pool.
    NoFreeSlots,

    /// Represents that an page_num does not exists on buffer pool.
    PageNotFound(PageNumber),

    /// Represents errors related to disk page access.
    Disk(pager::Error),
}

impl From<pager::Error> for Error {
    fn from(err: pager::Error) -> Self {
        Self::Disk(err)
    }
}

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

    /// Write at bytes buffer from a vec. Panic if data.len() > N.
    pub fn write_from_vec(&mut self, data: Vec<u8>) {
        self.write(self.vec_to_array(data));
    }

    /// Write the comming data overrinding the bytes buffer starting at the given offset.
    pub fn write_at(&mut self, data: &Vec<u8>, offset: usize) {
        assert!(
            data.len() <= self.data.len() + offset,
            "Data overflow the current buffer size"
        );

        let mut idx_outer = 0;
        for idx in offset..self.data.len() {
            if idx_outer >= data.len() {
                break;
            }
            self.data[idx] = data[idx_outer];
            idx_outer += 1;
        }
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

    fn vec_to_array<T>(&self, v: Vec<T>) -> [T; N] {
        v.try_into().unwrap_or_else(|v: Vec<T>| {
            panic!("Expected a Vec of length {} but it was {}", N, v.len())
        })
    }
}

/// Page is represents a mutable reference counter to a fixed block of bytes.
pub type Page = Rc<RefCell<Bytes<PAGE_SIZE>>>;

/// Page buffer indetifier;
///
/// Hold the index of page buffer on buffer pool and descriptor state for a single shared page buffer.
pub struct BufferData {
    /// Buffer index number.
    id: usize,

    /// The page number that the buffer is currenclty holding.
    page_num: PageNumber,

    /// The relation owner of this buffer.
    rel: Relation,

    /// Flag informing if the page buffer is dirty. If true, the buffer pool should flush the page
    /// contents to disk before victim.
    is_dirty: bool,

    /// Reference counter to the page buffer.
    refcount: usize,
}

impl BufferData {
    fn new(id: usize, page_num: PageNumber, rel: Relation) -> Buffer {
        Rc::new(RefCell::new(Self {
            id,
            page_num,
            rel,
            is_dirty: false,
            refcount: 0,
        }))
    }
}

/// A mutable reference counter to BufferData.
pub type Buffer = Rc<RefCell<BufferData>>;

/// BufferPool is responsible for fetching database pages from the disk and storing them in memory.
/// The BufferPool can also write dirty pages out to disk when it is either explicitly instructed to do so
/// or when it needs to evict a page to make space for a new page.
pub struct BufferPool {
    /// Replacer used to find a page that can be removed from memory.
    lru: LRU<PageNumber>,

    /// Size of buffer pool.
    size: usize,

    /// An array of page blocks.
    page_table: Vec<Page>,

    /// A map of page number to a page buffer descriptor
    buffer_table: HashMap<PageNumber, Buffer>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(size: usize) -> Self {
        Self {
            size,
            lru: LRU::new(size),
            page_table: Vec::with_capacity(size),
            buffer_table: HashMap::with_capacity(size),
        }
    }

    /// Fetch a block page from disk and return the Buffer that holds the page data.
    ///
    /// If no buffer exists already, selects a replacement victim and evicts the old page.
    ///
    /// The returned buffer is pinned and is already marked as holding the desired page.
    pub fn fetch_buffer(&mut self, rel: &Relation, page_num: PageNumber) -> Result<Buffer, Error> {
        if let Some(buffer) = self.buffer_table.get(&page_num) {
            let buffer = buffer.clone();
            println!(
                "Page {} exists on memory on buffer {}",
                page_num,
                buffer.borrow().id
            );
            self.pin_buffer(&buffer);
            Ok(buffer)
        } else {
            if self.page_table.len() >= self.size {
                println!("Buffer pool is at full capacity {}", self.size);
                self.victim()?;
            }
            assert!(
                self.page_table.len() < self.size && self.buffer_table.len() < self.size,
                "Buffer pool exceeded the limit of {}",
                self.size
            );

            println!("Fething page {} from disk", page_num);

            // Create a new empty page and read the page data from disk.
            let mut page = Bytes::new();
            let mut pager = Pager::open(&rel.full_path())?;
            pager.read_page(page_num, &mut page.bytes_mut())?;

            // Add page on cache and pin the new buffer.
            self.page_table.push(Rc::new(RefCell::new(page)));
            let buffer = BufferData::new(self.page_table.len(), page_num, rel.clone());
            self.pin_buffer(&buffer);
            self.buffer_table.insert(page_num, buffer.clone());

            Ok(buffer)
        }
    }

    /// Return the page contents from a buffer.
    pub fn get_page(&self, buffer: &Buffer) -> Page {
        self.page_table[buffer.borrow().id - 1].clone()
    }

    /// Allocate a new empty page block on disk on the given relation. If the buffer pool is at full capacity,
    /// alloc_page will select a replacement victim to allocate the new page.
    ///
    /// The returned buffer is pinned and is already marked as holding the new page.
    ///
    /// Return error if no new pages could be created, otherwise the buffer.
    pub fn alloc_buffer(&mut self, rel: &Relation) -> Result<Buffer, Error> {
        let mut pager = Pager::open(&rel.full_path())?;
        let page_num = pager.allocate_page()?;
        self.fetch_buffer(rel, page_num)
    }

    /// Make the buffer available for replacement. The buffer is also unpined on lru if the ref count is 0.
    ///
    /// Return error if the buffer does not exists on buffer pool, None otherwise.
    pub fn unpin_buffer(&mut self, buffer: Buffer, is_dirty: bool) -> Result<(), Error> {
        let mut buffer = buffer.borrow_mut();

        buffer.is_dirty = buffer.is_dirty || is_dirty;
        buffer.refcount -= 1;

        if buffer.refcount == 0 {
            self.lru.unpin(&buffer.page_num);
        }
        Ok(())
    }

    /// Make buffer unavailable for replacement.
    fn pin_buffer(&mut self, buffer: &Buffer) {
        let mut buffer = buffer.borrow_mut();
        buffer.refcount += 1;
        self.lru.pin(&buffer.page_num);
    }

    /// Physically write out a shared page to disk.
    ///
    /// Return error if the page could not be found in the page table, None otherwise.
    pub fn flush_buffer(&mut self, buffer: &Buffer) -> Result<(), Error> {
        let page = self.get_page(&buffer);

        let buffer = buffer.borrow();
        let mut pager = Pager::open(&buffer.rel.full_path())?;
        pager.write_page(buffer.page_num, &page.borrow().bytes())?;

        Ok(())
    }

    /// Physically write out a all shared pages stored on buffer pool to disk.
    pub fn flush_all_buffers(&mut self) -> Result<(), Error> {
        println!("Flushing all buffers to disk");
        for (_, buf) in self.buffer_table.iter() {
            let page = self.get_page(&buf);

            let buf = buf.borrow();
            let mut pager = Pager::open(&buf.rel.full_path())?;
            pager.write_page(buf.page_num, &page.borrow().bytes())?;
        }
        Ok(())
    }

    /// Use the LRU replacement policy to choose a page to victim. This function panic if the LRU
    /// don't have any page id to victim. Otherwise the page will be removed from page table. If
    /// the choosen page is dirty victim will flush to disk before removing from page table.
    fn victim(&mut self) -> Result<(), Error> {
        let page_num = self
            .lru
            .victim()
            .expect("replacer does not contain any page id to victim");

        println!("Page {} was chosen for victim", page_num);

        let buffer = self.get_buffer(page_num)?;
        let buffer = buffer.clone();

        if buffer.borrow().is_dirty {
            println!("Flusing dirty page {} to disk before victim", page_num);
            self.flush_buffer(&buffer)?;
        }

        let bufid = buffer.borrow().id;
        self.page_table.remove(bufid);
        self.buffer_table.remove(&page_num);

        Ok(())
    }

    /// Return the requested buffer descriptor to the given page id. If the page does not exists on buffer pool
    /// return Error::PageNotFound.
    fn get_buffer(&self, page_num: PageNumber) -> Result<Buffer, Error> {
        if let Some(buffer) = self.buffer_table.get(&page_num) {
            Ok(buffer.clone())
        } else {
            Err(Error::PageNotFound(page_num))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::rel::RelationData;

    use super::pager::PAGE_SIZE;
    use super::*;

    #[test]
    fn test_buffer_pool_write_dirty_page_on_victim() -> Result<(), Error> {
        let relation = test_relation(20);
        let buffer_pool_size = 3;
        let mut buffer_pool = BufferPool::new(buffer_pool_size);

        let page_data = [5; PAGE_SIZE];

        // Fetch a page from disk to memory, and write some data.
        {
            let buffer = buffer_pool.fetch_buffer(&relation, 1)?;
            let page = buffer_pool.get_page(&buffer);
            page.borrow_mut().write(page_data);
            buffer_pool.unpin_buffer(buffer, true)?;
        }

        // Fill buffer pool cache
        for page_num in 1..buffer_pool_size + 2 {
            let buffer = buffer_pool.fetch_buffer(&relation, page_num as u32)?;
            buffer_pool.unpin_buffer(buffer, false)?;
        }

        let buffer = buffer_pool.fetch_buffer(&relation, 1)?;
        let page = buffer_pool.get_page(&buffer);

        assert_eq!(
            page_data,
            page.borrow().bytes(),
            "Expected equal page data after victim dirty page"
        );

        Ok(())
    }

    #[test]
    fn test_buffer_pool_victin_on_fetch_page() -> Result<(), Error> {
        let relation = test_relation(20);
        let buffer_pool_size = 3;
        let mut buffer_pool = BufferPool::new(buffer_pool_size);

        // Fetch a page from disk to memory, and keep their reference.
        let _buffer = buffer_pool.fetch_buffer(&relation, 1)?;

        // Fill buffer pool cache
        for page_num in 1..buffer_pool_size + 1 {
            // Fetch some pages from disk to memory and make them
            // ready to victim.
            //
            // Note that since we fetch the page 1 before, after read
            // page 1 again and call unpin_page, the page 1 **should**
            // not be maked as ready for victim.
            let buffer = buffer_pool.fetch_buffer(&relation, page_num as u32)?;
            buffer_pool.unpin_buffer(buffer, false)?;
        }

        // Should victim some page and cache the new page.
        let _ = buffer_pool.fetch_buffer(&relation, 10)?;

        // Since the buffer pool reached maximum capacity the page table
        // should have the same size of buffer pool.
        assert_eq!(
            buffer_pool_size,
            buffer_pool.page_table.len(),
            "Expected that page table from buffer pool has the same size of buffer pool"
        );

        Ok(())
    }

    #[test]
    fn test_buffer_pool_fetch_page_from_memory() -> Result<(), Error> {
        let mut buffer = BufferPool::new(10);
        let buffer_from_disk = buffer.fetch_buffer(&test_relation(20), 5)?;
        let page_from_disk = buffer.get_page(&buffer_from_disk);

        let buffer_from_memory = buffer.fetch_buffer(&test_relation(20), 5)?;
        let page_from_memory = buffer.get_page(&buffer_from_memory);

        assert_eq!(page_from_disk, page_from_memory);

        Ok(())
    }

    #[test]
    fn test_buffer_pool_fetch_page_from_disk() -> Result<(), Error> {
        let mut buffer_pool = BufferPool::new(10);
        let buffer = buffer_pool.fetch_buffer(&test_relation(20), 5)?;
        let page = buffer_pool.get_page(&buffer);

        assert_eq!(page.borrow().bytes(), [4; PAGE_SIZE]);

        Ok(())
    }

    /// Create a new pager with a some empty pages.
    fn test_relation(pages: usize) -> Relation {
        use rand::prelude::random;

        let relation = RelationData {
            db_data: String::from(""),
            db_name: std::env::temp_dir().to_str().unwrap().to_string(),
            rel_name: format!("tinydb-tempfile-test-{}", random::<i32>()).to_string(),
        };

        let mut pager = Pager::open(&relation.full_path()).unwrap();

        for i in 0..pages {
            let page_number = pager.allocate_page().unwrap();
            let page_data = [i as u8; PAGE_SIZE];
            pager.write_page(page_number, &page_data).unwrap();
        }

        Rc::new(relation)
    }
}
