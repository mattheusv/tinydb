mod bytes;

use crate::disk::{self, PageNumber, Pager, PAGE_SIZE};
use crate::lru::LRU;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

use self::bytes::Bytes;

/// Represents errors that buffer pool can have.
#[derive(Debug)]
pub enum Error {
    /// Represents no free slots on buffer pool.
    NoFreeSlots,

    /// Represents that an page_id does not exists on buffer pool.
    PageNotFound(PageNumber),

    /// Represents errors related to disk page access.
    Disk(disk::Error),
}

impl From<disk::Error> for Error {
    fn from(err: disk::Error) -> Self {
        Self::Disk(err)
    }
}

/// Page is represents a mutable reference counter to a fixed block of bytes.
pub type Page = Rc<RefCell<Bytes<PAGE_SIZE>>>;

/// Wrapper type around BufferPoolEntry that represents a mutable reference to it.
type Entry = Rc<RefCell<BufferPoolEntry>>;

/// BufferPoolEntry represents an entry cache inside buffer pool. It hols the page beeing
/// cached, a flag informing if the page is dirty and the pin count for futher victim's.
struct BufferPoolEntry {
    page: Page,
    is_dirty: bool,
    count: usize,
}

impl BufferPoolEntry {
    /// Increment the pin count of entry.
    fn pin(&mut self) {
        self.count += 1;
    }

    /// Decrement the pin count of entry.
    fn unpin(&mut self) {
        self.count -= 1;
    }
}

impl Default for BufferPoolEntry {
    fn default() -> Self {
        Self {
            page: Rc::new(RefCell::new(Bytes::new())),
            is_dirty: false,
            count: 0,
        }
    }
}

impl Debug for BufferPoolEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("page", &"[...]")
            .field("is_dirty", &self.is_dirty)
            .field("count", &self.count)
            .finish()
    }
}

/// BufferPool is responsible for fetching database pages from the disk and storing them in memory.
/// The BufferPool can also write dirty pages out to disk when it is either explicitly instructed to do so
/// or when it needs to evict a page to make space for a new page.
pub struct BufferPool {
    /// Disk manager used to read and write pages in disk.
    disk: Pager,

    /// Replacer used to find a page that can be removed from memory.
    lru: LRU<PageNumber>,

    /// Size of buffer pool.
    size: usize,

    /// Contains a map of page number and buffer pool entry that is currecntly in use.
    page_table: HashMap<PageNumber, Entry>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(disk: Pager, size: usize) -> Self {
        Self {
            disk,
            size,
            page_table: HashMap::with_capacity(size),
            lru: LRU::new(size),
        }
    }

    /// Fetch the requested page from the buffer pool.
    pub fn fetch_page(&mut self, page_id: PageNumber) -> Result<Page, Error> {
        if let Ok(entry) = self.get_entry(page_id) {
            println!("Page {} exists on memory", page_id);
            entry.borrow_mut().pin();
            return Ok(entry.borrow().page.clone());
        }
        self.new_page(page_id)
    }

    /// Unpin the target page from the buffer pool. The page is also unpined on lru replacer
    /// if the pin count is 0.
    ///
    /// Return error if the page does not exists on buffer pool, None otherwise.
    pub fn unpin_page(&mut self, page_id: PageNumber, is_dirty: bool) -> Result<(), Error> {
        let entry = self.get_entry(page_id)?;

        let mut entry = entry.borrow_mut();
        entry.is_dirty = entry.is_dirty || is_dirty;
        entry.unpin();

        if entry.count == 0 {
            self.lru.unpin(&page_id);
        }

        Ok(())
    }

    /// Flushes the target page to disk.
    ///
    /// Return error if the page could not be found in the page table, None otherwise.
    pub fn flush_page(&mut self, page_id: PageNumber) -> Result<(), Error> {
        let entry = self.get_entry(page_id)?;
        self.disk
            .write_page(page_id, &entry.borrow().page.borrow().bytes())?;

        entry.borrow_mut().is_dirty = false;
        entry.borrow().page.borrow_mut().reset();

        Ok(())
    }

    /// Creates a new page in the buffer pool.
    ///
    /// Return error if no new pages could be created, otherwise the page.
    pub fn new_page(&mut self, page_id: PageNumber) -> Result<Page, Error> {
        if self.page_table.len() >= self.size {
            println!("Buffer pool is at full capacity {}", self.size);
            self.victim()?;
        }
        println!("Fething page {} from disk", page_id);

        let entry = Entry::default();
        entry.borrow_mut().pin();

        // Read the page from disk inside the buffer pool entry.
        self.disk
            .read_page(page_id, &mut entry.borrow().page.borrow_mut().bytes_mut())?;
        let page = entry.borrow().page.clone();

        // Add page on cache.
        self.page_table.insert(page_id, entry);
        self.lru.pin(&page_id);

        Ok(page)
    }

    /// Deletes a page from the buffer pool.
    ///
    /// Return error if the page exists but could not be deleted, None if the page didn't exist or
    /// deletion succeeded
    pub fn delete_page(&mut self, page_id: PageNumber) -> Result<(), Error> {
        if let Some(_) = self.page_table.remove(&page_id) {
            Ok(())
        } else {
            Err(Error::PageNotFound(page_id))
        }
    }

    /// Flushes all the pages in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) -> Result<(), Error> {
        todo!()
    }

    /// Use the LRU replacement policy to choose a page to victim. This function panic if the LRU
    /// don't have any page id to victim. Otherwise the page will be removed from page table. If
    /// the choosen page is dirty victim will flush to disk before removing from page table.
    fn victim(&mut self) -> Result<(), Error> {
        let page_id = self
            .lru
            .victim()
            .expect("replacer does not contain any page id to victim");

        println!("Page {} was chosen for victim", page_id);

        let entry = self.get_entry(page_id)?;

        if entry.borrow().is_dirty {
            println!("Flusing dirty page {} to disk before victim", page_id);
            self.flush_page(page_id)?;
        }

        self.page_table.remove(&page_id).expect(&format!(
            "page {} was chosen for victim but does not exists on page map",
            page_id
        ));

        Ok(())
    }

    /// Return the requested entry to the given page id. If the page does not exists on buffer pool
    /// get_page return Error::PageNotFound.
    fn get_entry(&self, page_id: PageNumber) -> Result<Entry, Error> {
        if let Some(entry) = self.page_table.get(&page_id) {
            return Ok(entry.clone());
        }
        Err(Error::PageNotFound(page_id))
    }
}

#[cfg(test)]
mod tests {
    use super::disk::PAGE_SIZE;
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_buffer_pool_write_dirty_page_on_victim() -> Result<(), Error> {
        let buffer_pool_size = 3;
        let mut buffer = BufferPool::new(open_test_pager(20), buffer_pool_size);

        let page_data = [5; PAGE_SIZE];

        // Fetch a page from disk to memory, and write some data.
        {
            let page = buffer.fetch_page(1)?;
            page.borrow_mut().write(page_data);
            buffer.unpin_page(1, true)?;
        }

        // Fill buffer pool cache
        for page_id in 1..buffer_pool_size + 2 {
            let _ = buffer.fetch_page(page_id as u32)?;
            buffer.unpin_page(page_id as u32, false)?;
        }

        let page = buffer.fetch_page(1)?;

        assert_eq!(
            page_data,
            page.borrow().bytes(),
            "Expected equal page data after victim dirty page"
        );

        Ok(())
    }

    #[test]
    fn test_buffer_pool_victin_on_fetch_page() -> Result<(), Error> {
        let buffer_pool_size = 3;
        let mut buffer = BufferPool::new(open_test_pager(20), buffer_pool_size);

        // Fetch a page from disk to memory, and keep their reference.
        let _page = buffer.fetch_page(1)?;

        // Fill buffer pool cache
        for page_id in 1..buffer_pool_size + 1 {
            // Fetch some pages from disk to memory and make them
            // ready to victim.
            //
            // Note that since we fetch the page 1 before, after read
            // page 1 again and call unpin_page, the page 1 **should**
            // not be maked as ready for victim.
            let _ = buffer.fetch_page(page_id as u32)?;
            buffer.unpin_page(page_id as u32, false)?;
        }

        // Should victim some page and cache the new page.
        let _ = buffer.fetch_page(10)?;

        // Since the buffer pool reached maximum capacity the page table
        // should have the same size of buffer pool.
        assert_eq!(
            buffer_pool_size,
            buffer.page_table.len(),
            "Expected that page table from buffer pool has the same size of buffer pool"
        );

        Ok(())
    }

    #[test]
    fn test_buffer_pool_fetch_page_from_memory() -> Result<(), Error> {
        let mut buffer = BufferPool::new(open_test_pager(20), 10);
        let page_from_disk = buffer.fetch_page(5)?;
        let page_from_memory = buffer.fetch_page(5)?;

        assert_eq!(page_from_disk, page_from_memory);

        Ok(())
    }

    #[test]
    fn test_buffer_pool_fetch_page_from_disk() -> Result<(), Error> {
        let mut buffer = BufferPool::new(open_test_pager(20), 10);
        let page = buffer.fetch_page(5)?;

        assert_eq!(page.borrow().bytes(), [4; PAGE_SIZE]);

        Ok(())
    }

    /// Create a new pager with a some empty pages.
    fn open_test_pager(total_pages: usize) -> Pager {
        let file = NamedTempFile::new().unwrap();
        let mut pager = Pager::open(file.path()).unwrap();

        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page();
            let page_data = [i as u8; PAGE_SIZE];
            pager.write_page(page_number, &page_data).unwrap();
        }

        pager
    }
}
