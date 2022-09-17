pub mod buffer;
pub mod bufpage;
pub mod freespace;
pub mod relation_locator;
pub mod smgr;

pub use buffer::BufferPool;

/// Each data file (heap or index) is divided into disk blocks
/// (which may be thought of as the unit of i/o -- a Bytes buffer
/// contains exactly one disk block). The blocks are numbered
/// sequentially, starting at 0.
///
/// The access methods, the buffer manager and the storage manager are
/// more or less the only pieces of code that should be accessing disk
/// blocks directly.
pub type PageNumber = u32;

/// Represents that a MemPage doest not exists on disk.
pub const INVALID_PAGE_NUMBER: PageNumber = 0;

/// Represents the size that a Page can have on database file.
pub const PAGE_SIZE: usize = 8192;

/// MemPage is a type that represents the array of bytes of some page in database.
pub type MemPage = [u8; PAGE_SIZE];
