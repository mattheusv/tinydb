/// A least recently used (LRU) implementation.
///
// Using LinkedHashList to implement LRU algorithm
//
// The current implementation does not perform well.
// All FrameIDs is stored in a Vec and when pin is called is
// necessary to iterate over all FrameIDs to remove it.
//
// TODO: Make this implementation thread safe.
pub struct LRU<T: Hash + Eq + Clone> {
    list: LinkedHashList<T, ()>,
}

impl<T> LRU<T>
where
    T: Hash + Eq + Clone + std::fmt::Debug,
{
    /// Create a new empty LruReplacer.
    pub fn new(size: usize) -> Self {
        Self {
            list: LinkedHashList::new(),
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
        self.list.pop_back().map(|(k, _)| k)
    }

    /// Remove the frame containing the pinned page from the LRUReplacer.
    ///
    /// This method should be called after a page is pinned to a frame.
    //
    // Technilly this function will be called when buffer pool page is pinned
    // to a frame, which means that a page was be shared between with a client,
    // so since the page is shared we can not remove from buffer pool cache.
    pub fn pin(&mut self, id: &T) {
        self.list.remove(id);
    }

    /// Add the frame containing the unpinned page to the LRUReplacer.
    ///
    /// This method should be called when the pin_count of a page becomes 0.
    //
    // Technilly this function will be called when a page do not have any references
    // to it (which means that your pin_count will be 0). If a Page/FrameID does not
    // have any references we can remove from cache.
    pub fn unpin(&mut self, id: &T) {
        self.list.push_front(id.clone(), ());
    }

    /// Returns the number of frames that are currently in the LRUReplacer.
    pub fn size(&self) -> usize {
        self.list.size()
    }

    /// Returns the elements in the LRUReplacer.
    pub fn elements(&self) -> Vec<T> {
        self.list
            .clone()
            .into_iter()
            .map(|node| node.key.clone())
            .collect()
    }
}

use std::iter::IntoIterator;
use std::{collections::HashMap, hash::Hash, ops::Deref, ops::DerefMut, ptr::NonNull};

struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<NodePtr<K, V>>,
    next: Option<NodePtr<K, V>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

struct NodePtr<K, V> {
    ptr: NonNull<Node<K, V>>,
}

impl<K, V> NodePtr<K, V> {
    fn new(node: &Node<K, V>) -> Self {
        Self {
            ptr: NonNull::from(node),
        }
    }
}

impl<K, V> Deref for NodePtr<K, V> {
    type Target = Node<K, V>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<K, V> DerefMut for NodePtr<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<K, V> Clone for NodePtr<K, V> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

impl<K, V> Copy for NodePtr<K, V> {}

impl<K: Hash, V> Hash for NodePtr<K, V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<K: PartialEq, V> PartialEq for NodePtr<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Eq, V> Eq for NodePtr<K, V> {}

unsafe impl<K, V> Send for NodePtr<K, V> {}

unsafe impl<K, V> Sync for NodePtr<K, V> {}

#[derive(Clone)]
struct LinkedHashList<K, V> {
    map: HashMap<NodePtr<K, V>, NodePtr<K, V>>,
    head: Option<NodePtr<K, V>>,
    tail: Option<NodePtr<K, V>>,
    size: usize,
}

impl<K, V> LinkedHashList<K, V>
where
    K: Hash + Eq + Clone,
    V: Default,
{
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            head: None,
            tail: None,
            size: 0,
        }
    }
    fn get(&mut self, key: &K) -> Option<&V> {
        let key = NodePtr::new(&Node::new(key.clone(), V::default()));
        if let Some(node) = self.map.get(&key).cloned() {
            // If the node is not the head, detach it from the list and attach it to the head.
            if self.head != Some(node) {
                self.deattach(node);
                self.attach(node);
            }
            unsafe { Some(&node.ptr.as_ref().value) }
        } else {
            None
        }
    }
    fn remove(&mut self, key: &K) -> Option<V> {
        let key = NodePtr::new(&Node::new(key.clone(), V::default()));
        if let Some(node) = self.map.remove(&key) {
            self.deattach(node);
            let node = unsafe { Box::from_raw(node.ptr.as_ptr()) };
            Some(node.value)
        } else {
            None
        }
    }
    fn deattach(&mut self, node: NodePtr<K, V>) {
        if let Some(mut prev) = node.prev {
            // If there is a predecessor, delete it from the list.
            if let Some(mut next) = node.next {
                prev.next = Some(next);
                next.prev = Some(prev);
            } else {
                prev.next = None;
                self.tail = Some(prev);
            }
        } else {
            // Only one element in the list.
            self.head = None;
            self.tail = None;
        }
        self.size -= 1;
    }
    fn attach(&mut self, mut node: NodePtr<K, V>) {
        // Attach the node to the head of the list.
        if let Some(mut head) = self.head {
            head.prev = Some(node);
            node.next = Some(head);
        }
        node.prev = None;
        self.head = Some(node);
        if self.tail.is_none() {
            node.next = None;
            self.tail = Some(node);
        }
        self.size += 1;
    }
    fn push_front(&mut self, key: K, value: V) {
        let node = Box::new(Node::new(key, value));
        let node = NonNull::from(Box::leak(node));
        let node_ptr = unsafe { NodePtr::new(node.as_ref()) };
        if let Some(_) = self.map.get(&node_ptr) {
            return;
        }
        self.attach(node_ptr);
        self.map.insert(node_ptr, node_ptr);
    }
    fn pop_back(&mut self) -> Option<(K, V)> {
        if let Some(tail) = self.tail {
            self.deattach(tail);
            self.map.remove(&tail);
            self.tail = tail.prev;
            let tail = unsafe { Box::from_raw(tail.ptr.as_ptr()) };
            Some((tail.key, tail.value))
        } else {
            None
        }
    }
    fn size(&self) -> usize {
        self.size
    }
}

struct LinkedHashListIterator<K, V> {
    list: LinkedHashList<K, V>,
    current: Option<NodePtr<K, V>>,
}

impl<K, V> Iterator for LinkedHashListIterator<K, V>
where
    K: Hash + Eq + Clone,
    V: Default,
{
    type Item = NodePtr<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current {
            self.current = current.next;
            Some(current)
        } else {
            None
        }
    }
}

impl<K, V> IntoIterator for LinkedHashList<K, V>
where
    K: Hash + Eq + Clone,
    V: Default,
{
    type Item = NodePtr<K, V>;
    type IntoIter = LinkedHashListIterator<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let current = self.head;
        LinkedHashListIterator {
            list: self,
            current: current,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unpin_same_key() {
        let mut replacer = LRU::new(5);
        replacer.unpin(&1);
        replacer.unpin(&2);
        replacer.unpin(&1); // Duplicate key
        replacer.unpin(&3);

        assert_eq!(replacer.victim(), Some(1));
        assert_eq!(replacer.victim(), Some(2));
        assert_eq!(replacer.victim(), Some(3));
        assert_eq!(replacer.size(), 0);
    }

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

        assert_eq!(replacer.elements(), vec![9, 8, 7, 6, 4, 2, 1, 0]);
        let _ = replacer.victim();
        assert_eq!(replacer.elements(), vec![9, 8, 7, 6, 4, 2, 1]);
    }

    #[test]
    fn test_lru_unpin() {
        let mut replacer = LRU::new(1);
        replacer.unpin(&1);
        assert_eq!(replacer.size(), 1);
    }
}
