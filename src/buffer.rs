use crate::pager::{self, MemPage, PageNumber, Pager};
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::rc::Rc;

/// Represents the ID of some page on the list of frames on buffer bool.
pub type FrameID = u32;

/// A least recently used (LRU) implementation.
///
// TODO: Improve the implementation.
//
// The current implementation does not perform well.
// All FrameIDs is stored in a Vec and when pin is called is
// necessary to iterate over all FrameIDs to remove it.
//
// TODO: Make this implementation thread safe.
pub struct LruReplacer {
    elements: Vec<FrameID>,
}

impl LruReplacer {
    /// Create a new empty LruReplacer.
    pub fn new(size: usize) -> Self {
        Self {
            elements: Vec::with_capacity(size),
        }
    }

    /// Remove the object that was accessed least recently compared
    /// to all the other elements being tracked by the Replacer, and
    /// return its contents. If the LruReplacer is empty None.
    //
    // Technilly, the buffer pool call this function when hit the max
    // capacity, then a FrameID will be returned contaning the frame id
    // that buffer pool should remove from cache. Note that the FrameID
    // returned will be also removed from LruReplacer internal data structure.
    pub fn victim(&mut self) -> Option<FrameID> {
        self.elements.pop()
    }

    /// Remove the frame containing the pinned page from the LRUReplacer.
    ///
    /// This method should be called after a page is pinned to a frame
    /// Technillyin the BufferPoolManager.
    //
    // Technilly this function will be called when buffer pool page is pinned
    // to a frame, which means that a page was be shared between with a client,
    // so since the page is shared we can not remove from buffer pool cache.
    pub fn pin(&mut self, id: &FrameID) {
        if let Some(index) = self.elements.iter().position(|v| v == id) {
            self.elements.remove(index);
        }
    }

    /// Add the frame containing the unpinned page to the LRUReplacer.
    ///
    /// This method should be called when the pin_count of a page becomes 0.
    //
    // Technilly this function will be called when a page do not have any references
    // to it (which means that your pin_count will be 0). If a Page/FrameID does not
    // have any references we can remove from cache.
    pub fn unpin(&mut self, id: &FrameID) {
        self.elements.insert(0, id.clone());
    }

    /// Returns the number of frames that are currently in the LRUReplacer.
    pub fn size(&self) -> usize {
        self.elements.len()
    }
}

/// Represents errors that buffer pool can have.
#[derive(Debug)]
pub enum Error {
    /// Represents no free slots on buffer pool.
    NoFreeSlots,

    /// Represents errors related to disk page access.
    DiskPager(pager::Error),
}

impl From<pager::Error> for Error {
    fn from(err: pager::Error) -> Self {
        Self::DiskPager(err)
    }
}

pub type Page = Rc<RefCell<MemPage>>;

pub struct BufferPool {
    /// Disk manager used to read and write pages in disk.
    pager: Pager,

    /// Replacer used to find a page that can be removed from memory.
    lru: LruReplacer,

    /// Size of buffer pool.
    size: usize,

    /// Contains a map of page number to index on page_table.
    page_map: HashMap<PageNumber, usize>,

    /// Contains an array of in-memory pages.
    page_table: Vec<Page>,

    /// Contains the free slots of page table.
    free_slots: Vec<usize>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(pager: Pager, size: usize) -> Self {
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
            lru: LruReplacer::new(size),
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
        // Page exists in memory, return a reference to it.
        if let Some(idx) = self.page_map.get(&page_id) {
            return Ok(self.page_table[*idx].clone());
        }

        // Page does not exists in memory. Find a free slot on page table
        // to read the page from disk.
        let free_slot = self.find_free_slot()?;
        let rc_page = self
            .page_table
            .get(free_slot)
            .expect("invalid free slot on page table");

        // Read from disk.
        let mut page = rc_page.borrow_mut();
        self.pager.read_page(page_id, &mut page)?;

        // Add on cache.
        self.page_map.insert(page_id.clone(), free_slot);

        Ok(rc_page.clone())
    }

    /// Return a free slot of page on page table. If there is no free slot
    /// Err(Error::NoFreeSlots) is returned.
    fn find_free_slot(&mut self) -> Result<usize, Error> {
        if let Some(slot) = self.free_slots.pop() {
            return Ok(slot);
        }
        Err(Error::NoFreeSlots)
    }
}

#[cfg(test)]
mod tests {
    use super::pager::{PageData, PAGE_SIZE};
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_buffer_pool_fetch_page() -> Result<(), Error> {
        let mut buffer = BufferPool::new(open_test_pager(20), 10);
        let page = buffer.fetch_page(5)?;

        let mut expected = MemPage::default();
        expected.set(5, [4; PAGE_SIZE]);

        assert_eq!(page, Rc::new(RefCell::new(expected)));

        Ok(())
    }

    #[test]
    fn test_replacer_victim() {
        let mut replacer = LruReplacer::new(3);
        replacer.unpin(&10);
        replacer.unpin(&30);
        replacer.unpin(&20);

        assert_eq!(replacer.victim(), Some(10));
        assert_eq!(replacer.victim(), Some(30));
        assert_eq!(replacer.victim(), Some(20));
        assert_eq!(replacer.victim(), None);
    }

    #[test]
    fn test_replacer_pin() {
        let mut replacer = LruReplacer::new(10);
        for i in 0..10 {
            replacer.unpin(&i);
        }
        assert_eq!(replacer.size(), 10);
        replacer.pin(&5);
        replacer.pin(&3);
        assert_eq!(replacer.size(), 8);
        assert_eq!(replacer.elements, vec![9, 8, 7, 6, 4, 2, 1, 0]);
        let _ = replacer.victim();
        assert_eq!(replacer.elements, vec![9, 8, 7, 6, 4, 2, 1]);
    }

    #[test]
    fn test_replacer_unpin() {
        let mut replacer = LruReplacer::new(1);
        replacer.unpin(&1);
        assert_eq!(replacer.size(), 1);
    }

    /// Create a new pager with a some empty pages.
    fn open_test_pager(total_pages: usize) -> Pager {
        let file = NamedTempFile::new().unwrap();
        let mut pager = Pager::open(file.path()).unwrap();

        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page();
            let page_data: PageData = [i as u8; PAGE_SIZE];
            let mem_page = MemPage {
                number: page_number,
                data: page_data,
            };
            pager.write_page(&mem_page).unwrap();
        }

        pager
    }
}
