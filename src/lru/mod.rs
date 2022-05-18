/// A least recently used (LRU) implementation.
///
// TODO: Improve the implementation.
//
// The current implementation does not perform well.
// All FrameIDs is stored in a Vec and when pin is called is
// necessary to iterate over all FrameIDs to remove it.
//
// TODO: Make this implementation thread safe.
#[derive(Debug, PartialEq)]
pub struct LRU<T> {
    elements: Vec<T>,
}

impl<T> LRU<T>
where
    T: Clone,
    T: PartialEq,
{
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
    pub fn victim(&mut self) -> Option<T> {
        self.elements.pop()
    }

    /// Remove the frame containing the pinned page from the LRUReplacer.
    ///
    /// This method should be called after a page is pinned to a frame.
    //
    // Technilly this function will be called when buffer pool page is pinned
    // to a frame, which means that a page was be shared between with a client,
    // so since the page is shared we can not remove from buffer pool cache.
    pub fn pin(&mut self, id: &T) {
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
    pub fn unpin(&mut self, id: &T) {
        self.elements.insert(0, id.clone());
    }

    /// Returns the number of frames that are currently in the LRUReplacer.
    pub fn size(&self) -> usize {
        self.elements.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_victim() {
        let mut replacer = LRU::new(3);
        replacer.unpin(&10);
        replacer.unpin(&30);
        replacer.unpin(&20);

        assert_eq!(replacer.victim(), Some(10));
        assert_eq!(replacer.victim(), Some(30));
        assert_eq!(replacer.victim(), Some(20));
        assert_eq!(replacer.victim(), None);
    }

    #[test]
    fn test_lru_pin() {
        let mut replacer = LRU::new(10);
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
    fn test_lru_unpin() {
        let mut replacer = LRU::new(1);
        replacer.unpin(&1);
        assert_eq!(replacer.size(), 1);
    }
}
