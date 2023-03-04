#![allow(dead_code)]
use std::borrow::Borrow;
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::{collections::HashMap, hash::Hash, ops::Deref, ops::DerefMut, ptr::NonNull};

/// LinkedHashList is implemented based on double linked list
/// and HashMap is used to implement O(1) remove and push operation
pub struct LinkedHashList<K, V> {
    // key and value are both NodePtr, so that we can share the key and the key doesn't need to implement Clone
    map: HashMap<NodePtr<K, V>, NodePtr<K, V>>,
    head: Option<NodePtr<K, V>>,
    tail: Option<NodePtr<K, V>>,
    size: usize,
    // Used to mark the lifetime of K and V, which will not exceed the lifetime of LinkedHashList
    marker: PhantomData<Node<K, V>>,
}

impl<K, V> LinkedHashList<K, V>
where
    K: Hash + Eq,
    V: Default,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
            size: 0,
            marker: PhantomData,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        // Cloned is required here, otherwise the node will be viewed as an immutable borrow
        // When deattach is reported, because deattach requires a mutable borrow
        match self.map.get(key).cloned() {
            Some(node) => {
                self.deattach(node);
                let node = unsafe { Box::from_raw(node.ptr.as_ptr()) };
                Some(node.value)
            }
            None => None,
        }
    }

    pub fn push_front(&mut self, key: K, value: V) {
        let node_ptr = unsafe { NodePtr::new_boxed(key, value) };
        // Disallow duplicate insertion
        if let Some(_) = self.map.get(&node_ptr) {
            return;
        }
        self.attach(node_ptr);
        self.map.insert(node_ptr, node_ptr);
    }
    pub fn pop_back(&mut self) -> Option<(K, V)> {
        match self.tail {
            Some(tail) => {
                self.deattach(tail);
                self.map.remove(&tail);
                self.tail = tail.prev;
                let tail = unsafe { Box::from_raw(tail.ptr.as_ptr()) };
                // Here the ownership of K and V is returned
                Some((tail.key, tail.value))
            }
            None => None,
        }
    }
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        match self.head {
            Some(head) => {
                self.deattach(head);
                self.map.remove(&head);
                self.head = head.next;
                let head = unsafe { Box::from_raw(head.ptr.as_ptr()) };
                Some((head.key, head.value))
            }
            None => None,
        }
    }
    pub fn size(&self) -> usize {
        self.size
    }
    fn attach(&mut self, mut node: NodePtr<K, V>) {
        self.size += 1;
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
    }
    fn deattach(&mut self, node: NodePtr<K, V>) {
        self.size -= 1;
        match node.prev {
            Some(mut prev) => {
                // If there is a predecessor node, delete the successor pointer of the predecessor node
                match node.next {
                    Some(mut next) => {
                        prev.next = Some(next);
                        next.prev = Some(prev);
                    }
                    None => {
                        prev.next = None;
                        self.tail = Some(prev);
                    }
                }
            }
            None => {
                // Only one node
                self.head = None;
                self.tail = None;
            }
        }
    }
    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            head: self.head,
            len: self.size,
            marker: PhantomData,
        }
    }
    pub fn iter_mut(&mut self) -> IterMut<K, V> {
        IterMut {
            head: self.head,
            len: self.size,
            marker: PhantomData,
        }
    }
}

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
// Raw pointer pointing to the node, wrapped in NonNull
// Since it is used in map, the Hash and Eq trait of K are forwarded to NodePtr
struct NodePtr<K, V> {
    ptr: NonNull<Node<K, V>>,
}

impl<K, V> NodePtr<K, V> {
    fn new(node: &Node<K, V>) -> Self {
        Self {
            ptr: NonNull::from(node),
        }
    }
    // Here unsafe is used because Box::leak is used, and Node <K, V> needs to be released
    unsafe fn new_boxed(key: K, value: V) -> Self {
        let node = Box::new(Node::new(key, value));
        let node = Box::leak(node);
        Self::new(node)
    }
}

// The lifetime of K is the same as the lifetime of NodePtr, and it is safe to borrow the reference of K
impl<K, V> Borrow<K> for NodePtr<K, V> {
    fn borrow(&self) -> &K {
        unsafe { &self.ptr.as_ref().key }
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

// If the lru is locked as a whole, the operation of NodePtr is thread-safe
unsafe impl<K, V> Send for NodePtr<K, V> {}

unsafe impl<K, V> Sync for NodePtr<K, V> {}

// Immutable iterator on LinkedHashList
pub struct Iter<'a, K, V> {
    head: Option<NodePtr<K, V>>,
    len: usize,
    // The lifetime of the iterator will not exceed the lifetime of the Node
    marker: PhantomData<&'a Node<K, V>>,
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        match self.head {
            Some(head) => {
                let node = unsafe { head.ptr.as_ref() };
                self.head = node.next;
                Some((&node.key, &node.value))
            }
            None => None,
        }
    }
}
// Mutable iterator on LinkedHashList
pub struct IterMut<'a, K: 'a, V: 'a> {
    head: Option<NodePtr<K, V>>,
    len: usize,
    // The lifetime of the iterator will not exceed the lifetime of the Node
    marker: PhantomData<&'a mut Node<K, V>>,
}
impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a mut K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        match self.head {
            Some(mut head) => {
                let node = unsafe { head.ptr.as_mut() };

                self.head = node.next;

                Some((&mut node.key, &mut node.value))
            }
            None => None,
        }
    }
}
// Iterator with ownership on LinkedHashList
pub struct IntoIter<K, V> {
    list: LinkedHashList<K, V>,
}

impl<K, V> Iterator for IntoIter<K, V>
where
    K: Hash + Eq,
    V: Default,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.list.pop_front() {
            Some(current)
        } else {
            None
        }
    }
}

impl<K, V> IntoIterator for LinkedHashList<K, V>
where
    K: Hash + Eq,
    V: Default,
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { list: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn push_pop() {
        let mut list = LinkedHashList::new(10);
        list.push_front(1, 1);
        list.push_front(2, 2);
        list.push_front(3, 3);
        list.push_front(4, 4);
        list.push_front(5, 5);
        assert_eq!(list.pop_front(), Some((5, 5)));
        assert_eq!(list.pop_front(), Some((4, 4)));
        assert_eq!(list.pop_front(), Some((3, 3)));
        assert_eq!(list.pop_front(), Some((2, 2)));
        assert_eq!(list.pop_front(), Some((1, 1)));
        assert_eq!(list.pop_front(), None);
    }
    #[test]
    fn remove() {
        let mut list = LinkedHashList::new(10);
        list.push_front(1, 1);
        list.push_front(2, 2);
        list.push_front(3, 3);
        list.push_front(4, 4);
        list.push_front(5, 5);
        list.remove(&3);
        assert_eq!(list.pop_front(), Some((5, 5)));
        assert_eq!(list.pop_front(), Some((4, 4)));
        assert_eq!(list.pop_front(), Some((2, 2)));
        assert_eq!(list.pop_front(), Some((1, 1)));
        assert_eq!(list.pop_front(), None);
    }
}
