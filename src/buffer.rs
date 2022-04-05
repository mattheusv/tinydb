use crate::pager::{self, MemPage, PageNumber, Pager, INVALID_PAGE_NUMBER, PAGE_SIZE};
use crate::replacer::LruReplacer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Represents errors that buffer pool can have.
#[derive(Debug)]
pub enum Error {
    /// Represents no free slots on buffer pool.
    NoFreeSlots,

    /// Represents that an page_id does not exists on buffer pool.
    PageNotFound(PageNumber),

    /// Represents errors related to disk page access.
    DiskPager(pager::Error),
}

impl From<pager::Error> for Error {
    fn from(err: pager::Error) -> Self {
        Self::DiskPager(err)
    }
}

pub type Page = Rc<RefCell<PageData>>;

/// Page is a wrapper arround a RcMemPage and RcLruReplacer that automatically unpin page from
/// replacer when necessary during the drop operations.
//
// TODO: Make this implementation thread safe.
#[derive(Debug, PartialEq)]
pub struct PageData {
    /// The current block of memory of a page.
    data: MemPage,

    /// ID of current page.
    number: PageNumber,

    /// Flag that means if page was changed in memory or not.
    is_dirty: bool,

    /// Number of readers that are using current page
    count: u32,
}

impl PageData {
    fn new(number: PageNumber, data: MemPage) -> Self {
        Self {
            data,
            number,
            is_dirty: false,
            count: 0,
        }
    }

    /// Write data on current in memory page instance.
    pub fn write(&mut self, data: MemPage) {
        self.data = data;
        self.is_dirty = true;
    }

    /// Reset page instance to INVALID_PAGE_NUMBER and empty page data.
    fn reset(&mut self) {
        assert!(!self.is_dirty, "Can not reset dirty page");
        self.data = [0; PAGE_SIZE];
        self.number = INVALID_PAGE_NUMBER;
        self.is_dirty = false;
    }

    fn pin(&mut self) {
        self.count += 1;
        println!("Page {} contains {} pins", self.number, self.count);
    }

    fn unpin(&mut self) {
        self.count -= 1;
        println!("Page {} contains {} pins", self.number, self.count);
    }
}

pub struct BufferPool {
    /// Disk manager used to read and write pages in disk.
    pager: Pager,

    /// Replacer used to find a page that can be removed from memory.
    replacer: LruReplacer,

    /// Size of buffer pool.
    size: usize,

    /// Contains a map of page number to index on page_table.
    page_map: HashMap<PageNumber, usize>,

    /// Contains an array of in-memory pages.
    page_table: Vec<Page>,

    /// Contains the index of free slots on page table.
    free_slots: Vec<usize>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(pager: Pager, size: usize) -> Self {
        let replacer = LruReplacer::new(size);

        let mut page_table = Vec::with_capacity(size);
        let mut free_slots = Vec::with_capacity(size);

        for idx in 0..size {
            page_table.push(Rc::new(RefCell::new(PageData::new(
                INVALID_PAGE_NUMBER,
                [0; PAGE_SIZE],
            ))));
            free_slots.push(idx);
        }

        Self {
            pager,
            size,
            page_table,
            free_slots,
            page_map: HashMap::with_capacity(size),
            replacer,
        }
    }

    /// Fetch the requested page from the buffer pool.
    pub fn fetch_page(&mut self, page_id: PageNumber) -> Result<Page, Error> {
        if let Ok(page) = self.get_page(page_id) {
            if page.borrow().number == page_id {
                println!("Page {} exists on memory", page_id);
                page.borrow_mut().pin();
                return Ok(page);
            }
        }
        println!("Fething page {} from disk", page_id);
        self.new_page(page_id)
    }

    /// Unpin the target page from the buffer pool. The page is also unpined on replacer
    /// if the pin count is 0.
    ///
    /// Return error if the page does not exists on buffer pool, None otherwise.
    pub fn unpin_page(&mut self, page_id: PageNumber) -> Result<(), Error> {
        let page = self.get_page(page_id)?;
        page.borrow_mut().unpin();

        if page.borrow().count == 0 {
            self.replacer.unpin(&page_id);
        }

        Ok(())
    }

    /// Flushes the target page to disk.
    ///
    /// The param page_id id cannot be INVALID_PAGE_ID.
    ///
    /// Return error if the page could not be found in the page table, None otherwise.
    pub fn flush_page(&mut self, page_id: PageNumber) -> Result<(), Error> {
        let page = self.get_page(page_id)?;
        let mut page = page.borrow_mut();
        self.pager.write_page(page_id, &page.data)?;
        page.is_dirty = false;
        Ok(())
    }

    /// Creates a new page in the buffer pool.
    ///
    /// Return error if no new pages could be created, otherwise the page
    pub fn new_page(&mut self, page_id: PageNumber) -> Result<Page, Error> {
        let free_slot = match self.find_free_slot() {
            Ok(slot) => slot,
            Err(Error::NoFreeSlots) => self.victim()?,
            Err(err) => return Err(err),
        };

        println!("Using free slot {} to store page {}", free_slot, page_id);

        let rc_page = self.page_table.get(free_slot).expect(&format!(
            "invalid free slot {} on page table of size {}",
            free_slot,
            self.page_table.len()
        ));

        // Fill page id and page data from pager.
        let mut page = rc_page.borrow_mut();
        page.number = page_id;
        self.pager.read_page(page_id, &mut page.data)?;

        // Add page on cache.
        self.page_map.insert(page_id, free_slot);

        page.pin();
        self.replacer.pin(&page_id);

        Ok(rc_page.clone())
    }

    /// Deletes a page from the buffer pool.
    ///
    /// Return error if the page exists but could not be deleted, None if the page didn't exist or deletion succeeded
    pub fn delete_page(&mut self, page_id: PageNumber) -> Result<(), Error> {
        if let Some(_) = self.page_map.remove(&page_id) {
            Ok(())
        } else {
            Err(Error::PageNotFound(page_id))
        }
    }

    /// Flushes all the pages in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) -> Result<(), Error> {
        todo!()
    }

    fn victim(&mut self) -> Result<usize, Error> {
        println!("Buffer pool is at full capacity {}", self.size);

        let page_id = self
            .replacer
            .victim()
            .expect("replacer does not contain any page id to victim");

        println!("Page {} was chosen for victim", page_id);

        let rc_page = self.get_page(page_id)?;

        if rc_page.borrow().is_dirty {
            println!("Flusing dirty page {} to disk before victim", page_id);
            self.flush_page(page_id)?;
        }

        rc_page.borrow_mut().reset();

        let free_slot = self.page_map.get(&page_id).expect(&format!(
            "page {} was chosen for victim but does not exists on page map",
            page_id
        ));

        Ok(*free_slot)
    }

    /// Return the free slot to store a page on page table. If there is no free slot
    /// find_free_slot return Error::NoFreeSlots.
    fn find_free_slot(&mut self) -> Result<usize, Error> {
        if let Some(slot) = self.free_slots.pop() {
            return Ok(slot);
        }
        Err(Error::NoFreeSlots)
    }

    /// Return the request page to the given page id. If the page does not exists on buffer pool
    /// get_page return Error::PageNotFound.
    fn get_page(&self, page_id: PageNumber) -> Result<Page, Error> {
        if let Some(idx) = self.page_map.get(&page_id) {
            let page = self.page_table[*idx].clone();
            return Ok(page);
        }
        Err(Error::PageNotFound(page_id))
    }
}

#[cfg(test)]
mod tests {
    use super::pager::PAGE_SIZE;
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
            buffer.unpin_page(1)?;
        }

        // Fill buffer pool cache
        for page_id in 1..buffer_pool_size + 2 {
            let _ = buffer.fetch_page(page_id as u32)?;
            buffer.unpin_page(page_id as u32)?;
        }

        let page = buffer.fetch_page(1)?;

        assert_eq!(
            page_data,
            page.borrow().data,
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
            buffer.unpin_page(page_id as u32)?;
        }

        // Should victim some page and cache the new page.
        let _ = buffer.fetch_page(10)?;

        // Since the buffer pool reached maximum capacity the buffer pool
        // should do not have any free slot to use.
        // Next pages that will be be cached, first is necessary victim
        // a page to reuse.
        assert_eq!(0, buffer.free_slots.len(), "Expected 0 free slot");

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

        assert_eq!(page.borrow().number, 5);
        assert_eq!(page.borrow().data, [4; PAGE_SIZE]);

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
