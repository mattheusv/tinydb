pub mod buffer;
pub mod disk;
pub mod freespace;
pub mod page;
pub mod relation_locator;
pub mod smgr;

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

/// Each data file (heap or index) is divided into disk
/// blocks, (which may be thought of as the unit of IO).
/// A Page contains exactly one disk block.
///
/// The storage manager is the only pieces of code that
/// should be accessing disk blocks directly.
///
/// Buffer pool and access methods normally works using
/// MemPage that contains the actual page data and other
/// helper methods.
pub type Page = [u8; PAGE_SIZE];
