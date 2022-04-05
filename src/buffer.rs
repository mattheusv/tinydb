use crate::pager::{self, MemPage, PageNumber, Pager};
use crate::replacer::LruReplacer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Minium value of strong references that a Page can have to unpim for victim's.
//
// Note that the value is 2 because we actually always will have at least an reference
// to Rc<T> and a other to call the strong_count function.
const MIN_STRONG_COUNT: usize = 2;

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

/// Represents a mutable reference counter of a replacer.
type RcLruReplacer = Rc<RefCell<LruReplacer>>;

/// Represents a mutable reference counter of a memory page.
type RcMemPage = Rc<RefCell<MemPage>>;

/// Page is a wrapper arround a RcMemPage and RcLruReplacer that automatically unpin page from
/// replacer when necessary during the drop operations.
//
// TODO: Make this implementation thread safe.
#[derive(Debug, PartialEq)]
pub struct Page {
    /// The current block of memory of a page.
    page: RcMemPage,

    /// replacer used to unpin the page when necessary.
    replacer: RcLruReplacer,
}

impl Page {
    fn new(page: RcMemPage, replacer: RcLruReplacer) -> Self {
        Self { page, replacer }
    }
}

impl Drop for Page {
    // Unpin an page from replacer if necessary. Basically this "unpining" will happen when a page
    // does not have any references except the reference to Rc itself and the variable used to check
    // the strong_count. dsadsa
    fn drop(&mut self) {
        let count = Rc::strong_count(&self.page);
        if count <= MIN_STRONG_COUNT {
            self.replacer
                .borrow_mut()
                .unpin(&self.page.borrow().number());
        }

        if count > MIN_STRONG_COUNT {
            // This drop calls can actually be a recursive drop starting from Rc.
            // Other calls of drop will be from callers that use the Rc for while and drop their
            // references, so only print in theses scenarios.
            println!(
                "Page {} contain {} referenes",
                self.page.borrow().number(),
                count - MIN_STRONG_COUNT
            );
        }
    }
}

pub struct BufferPool {
    /// Disk manager used to read and write pages in disk.
    pager: Pager,

    /// Replacer used to find a page that can be removed from memory.
    replacer: RcLruReplacer,

    /// Size of buffer pool.
    size: usize,

    /// Contains a map of page number to index on page_table.
    page_map: HashMap<PageNumber, usize>,

    /// Contains an array of in-memory pages.
    page_table: Vec<RcMemPage>,

    /// Contains the index of free slots on page table.
    free_slots: Vec<usize>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(pager: Pager, size: usize) -> Self {
        let replacer = Rc::new(RefCell::new(LruReplacer::new(size)));

        let mut page_table = Vec::with_capacity(size);
        let mut free_slots = Vec::with_capacity(size);

        for idx in 0..size {
            page_table.push(Rc::new(RefCell::new(MemPage::default())));
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

    /// Returns a Page object that contains the contents of the given page_id.
    /// The function first check its internal page table to see whether there already exists
    /// a Page that is mapped to the page_id. If it does, then it returns it.
    /// Otherwise it will retrieve the physical page from the [Pager]. To do this, the
    /// function select a [pager::MemPage] object to store the physical page's contents.
    /// If there are free frames in the page table, then the function will select a random one
    /// to use. Otherwise, it will use the LRUReplacer to select an unpinned [pager::MemPage] that was
    /// least recently used as the "victim" page. If there are no free slots
    /// (i.e., all the pages are pinned), then return an Error::NoFreeSlots.
    /// If the selected victim page is dirty, then the [Pager] is used to write its contents out
    /// to disk. The [Pager] is also used to read the target physical page from disk and copy its contents
    /// into that [pager::MemPage] object.
    pub fn fetch_page(&mut self, page_id: PageNumber) -> Result<Page, Error> {
        if let Ok(page) = self.get_page(&page_id) {
            // Page exists in memory, return a reference to it.
            println!("Page {} exists in memory", page_id);
            return Ok(Page::new(page, self.replacer.clone()));
        }
        println!(
            "Page {} does not exists in memory, fetching from disk",
            page_id
        );

        // Page does not exists in memory. Find a free slot on page table
        // to read the page from disk.
        let free_slot = match self.find_free_slot() {
            Ok(fs) => fs,
            Err(Error::NoFreeSlots) => {
                println!("Buffer pool is at full capacity {}", self.size);
                self.victim()?
            }
            Err(err) => return Err(err),
        };

        println!("Using free slot {} to store page {}", free_slot, page_id);

        let rc_page = self.page_table.get(free_slot).expect(&format!(
            "invalid free slot {} on page table of size {}",
            free_slot, self.size
        ));

        // Read from disk.
        let mut page = rc_page.borrow_mut();
        self.pager.read_page(page_id, &mut page)?;

        // Add on cache.
        self.page_map.insert(page_id.clone(), free_slot);

        // Pin this page to avoid victim for other calls.
        //
        // We can not victim this page when cache is full because the client
        // could been using this page, so we can not reuse. When client unpin
        // a page using unping_page method this page_id will be added again on
        // replacer using replacer.unpin(&page_id), which means that this page
        // could be victim for other fetch_page calls.
        self.replacer.borrow_mut().pin(&page_id);

        Ok(Page::new(rc_page.clone(), self.replacer.clone()))
    }

    /// Retrieve the Page object specified by the given page_id and then use the [Pager] to write its
    /// contents out to disk. Upon successful completion of that write operation, the function
    /// will return nothing. This function does not remove the Page from the buffer pool.
    /// If there is no entry in the page table for the given page_id, then return
    /// Err(Error::PageNotFound).
    ///
    /// Note that it also does not update the LRUReplacer for the Page.
    pub fn flush_page(&mut self, page_id: &PageNumber) -> Result<(), Error> {
        let page = self.get_page(page_id)?;
        self.pager.write_page(&page.borrow())?;
        Ok(())
    }

    /// Victim a page from cache using replacer strategy and return the new free slot.
    fn victim(&mut self) -> Result<usize, Error> {
        let victim_page = self
            .replacer
            .borrow_mut()
            .victim()
            .expect("replacer does not contain any page id to victim.");

        println!("Page {} was chosen for victim", victim_page);

        let page = self.get_page(&victim_page)?;

        // Write page on disk if is dirty.
        if page.borrow().is_dirty {
            println!(
                "Page {} is dirty, writing to disk before victim",
                victim_page
            );
            self.flush_page(&victim_page)?;
        }

        // Invalidate page data.
        page.borrow_mut().reset();

        // Get index of slot to be reused.
        let free_slot = self.page_map.get(&victim_page).expect(&format!(
            "victim page {} does not exists on page map {:?}",
            victim_page, self.page_map
        ));

        Ok(*free_slot)
    }

    /// Return a free slot of page on page table. If there is no free slot
    /// Err(Error::NoFreeSlots) is returned.
    fn find_free_slot(&mut self) -> Result<usize, Error> {
        if let Some(page_id) = self.free_slots.pop() {
            return Ok(page_id);
        }
        Err(Error::NoFreeSlots)
    }

    /// Return a page from page table to a given page id.
    /// If the page id does not exists on page on page table
    /// return Error::PageNotFound.
    fn get_page(&self, page_id: &PageNumber) -> Result<RcMemPage, Error> {
        if let Some(idx) = self.page_map.get(&page_id) {
            return Ok(self.page_table[*idx].clone());
        }
        Err(Error::PageNotFound(*page_id))
    }
}

#[cfg(test)]
mod tests {
    use super::pager::{PageData, PAGE_SIZE};
    use super::*;
    use tempfile::NamedTempFile;

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

        let expected = MemPage::new(5, [4; PAGE_SIZE]);

        assert_eq!(
            page,
            Page::new(Rc::new(RefCell::new(expected)), buffer.replacer.clone())
        );

        Ok(())
    }

    /// Create a new pager with a some empty pages.
    fn open_test_pager(total_pages: usize) -> Pager {
        let file = NamedTempFile::new().unwrap();
        let mut pager = Pager::open(file.path()).unwrap();

        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page();
            let page_data: PageData = [i as u8; PAGE_SIZE];
            let mem_page = MemPage::new(page_number, page_data);
            pager.write_page(&mem_page).unwrap();
        }

        pager
    }
}
