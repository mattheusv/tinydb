use anyhow::{bail, Result};
use log::debug;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{
    prelude::{Read, Write},
    Seek, SeekFrom,
};
use std::path::Path;

/// Represents the tinydb header size.
const HEADER_SIZE: usize = 100;

/// Represents the size that a Page can have on database file.
pub const PAGE_SIZE: usize = 8192;

/// Represents the first N bytes of the file.
pub const MAGIC_BYTES_SIZE: usize = 6;

/// Represents the first [MAGIC_BYTES_SIZE] of file.
pub const MAGIC_BYTES: &[u8; MAGIC_BYTES_SIZE] = b"Tinydb";

/// Represents that a MemPage doest not exists on disk.
pub const INVALID_PAGE_NUMBER: PageNumber = 0;

/// HeaderData is a type that represents the array of bytes
/// containing the header data from database file.
pub type HeaderData = [u8; HEADER_SIZE];

/// MemPage is a type that represents the array of bytes of some page in database.
pub type MemPage = [u8; PAGE_SIZE];

/// Each data file (heap or index) is divided into disk blocks
/// (which may be thought of as the unit of i/o -- a Bytes buffer
/// contains exactly one disk block). The blocks are numbered
/// sequentially, starting at 0.
///
/// The access methods, the buffer manager and the storage manager are
/// more or less the only pieces of code that should be accessing disk
/// blocks directly.
pub type PageNumber = u32;

/// Represents errors that pager can have.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    /// Represents an invalid page number on database file.
    #[error("")]
    IncorrectPageNumber,

    /// The database file is corrupted. Mostly the magic bytes
    /// is different than [MAGIC_BYTES].
    #[error("")]
    CorruptedFile,

    /// Could not convert a type to bytes representation.
    #[error("")]
    Serialize(String),
}

/// A in memory representation of a pager file header.
///
/// Note that Header instances are in-memory copy of current
/// page header data, if change was made is necessary to write
/// back to disk using [write_header](Pager::write_header) function.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Header {
    magic: [u8; MAGIC_BYTES_SIZE],
}

impl Header {
    /// Return the fixed size byte representation of header file.
    ///
    /// Note that if [Header] size is less than [HEADER_SIZE] the
    /// bytes data will be resized and 0 values will be added in
    /// the end of array. If [Header] size is greater thatn [HEADER_SZIE]
    /// the function will panic. Truncate the slice can't be done because
    /// can lost values and generate bugs.
    pub fn serialize(&self) -> Result<HeaderData> {
        let mut data = bincode::serialize(self)?;
        if data.len() < HEADER_SIZE {
            data.resize(HEADER_SIZE, 0);
        }
        Ok(data.try_into().unwrap_or_else(|v: Vec<u8>| {
            panic!(
                "Expected a Header of length {} but it was {}",
                HEADER_SIZE,
                v.len()
            )
        }))
    }

    /// Convert a fixed size byte array to Header.
    pub fn deserialize(data: &HeaderData) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            magic: MAGIC_BYTES.clone(),
        }
    }
}

/// Pager handle all read/write operations on database file.
///
/// If you want to modify the file, you need to modify the page returned by
/// the pager and instruct the pager to write it back to disk.
///
/// The Pager is very simple and always creates an in-memory copy of any page
/// that is read (even if that page has already been read before).
/// More specifically, pages are read into a MemPage structure.
#[derive(Debug)]
pub struct Pager {
    file: File,
    total_pages: u32,
}

impl Pager {
    /// Open a file for paged access.
    ///
    /// This function opens a database file and verifies that the file
    /// header is correct. If the file is empty (which will happen if the
    /// pager is given a filename for a file that does not exist) then this
    /// function will initialize the file header using the default values.
    pub fn open(filename: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)?;
        let mut pager = Self {
            file,
            total_pages: 0,
        };
        pager.total_pages = pager.size()?;

        if pager.is_empty()? {
            pager.initialize_header()?;
        } else {
            pager.validate_header()?;
        }
        Ok(pager)
    }

    /// Read a page from file.  This pager reads a page from the disk,
    /// and updates the in-memory MemPage struct passed on page arg.
    /// Any changes done to a MemPage will not be effective until call
    /// the [write_page](Pager::write_page] with that MemPage.
    pub fn read_page(&mut self, page_number: PageNumber, page: &mut MemPage) -> Result<()> {
        self.validate_page(page_number)?;
        self.file.seek(SeekFrom::Start(self.offset(page_number)))?;
        let count = self.file.read(page)?;
        debug!("Read {} bytes from page {}", count, page_number);
        Ok(())
    }

    /// Write a page to file. This pager writes the in-memory copy of a
    /// page (stored in a MemPage struct) back to disk.
    pub fn write_page(&mut self, number: PageNumber, page: &MemPage) -> Result<()> {
        self.validate_page(number)?;
        self.file.seek(SeekFrom::Start(self.offset(number)))?;
        let count = self.file.write(page)?;
        debug!("Wrote {} bytes to page {}", count, number);
        Ok(())
    }

    /// Allocate an extra page on the file and returns the page number
    pub fn allocate_page(&mut self) -> Result<u32> {
        self.total_pages += 1;
        self.write_page(self.total_pages, &[0; PAGE_SIZE])?;
        Ok(self.total_pages)
    }

    /// Reads the header of database file and returns it in a byte array.
    /// Note that this function can be called even if the page size is unknown,
    /// since the chidb header always occupies the first 100 bytes of the file.
    pub fn read_header(&mut self) -> Result<Header> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        self.file.read(&mut header)?;
        Ok(Header::deserialize(&header)?)
    }

    /// Write the header on database file. Note that the write_header function will
    /// always override the current header data if exists.
    pub fn write_header(&mut self, header: &Header) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write(&header.serialize()?)?;
        Ok(())
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32> {
        let len = self.file.metadata()?.len();
        if len == 0 || len as usize - HEADER_SIZE == 0 {
            // If len is equal 0 means that the file is empty.
            // If len - HEADER_SIZE is equal 0 means that the
            // file doest not have any page, so in both case
            // return 0.
            return Ok(0);
        }
        // Otherwise we calculate the total of
        // pages in file and finally substract with the
        // HEADER_SIZE to get the total of pages in file.
        Ok((len as u32 / PAGE_SIZE as u32) - HEADER_SIZE as u32)
    }

    /// Check if a pager number is valid to this database file buffer.
    fn validate_page(&self, page: PageNumber) -> Result<()> {
        if page > self.total_pages || page <= 0 {
            bail!(Error::IncorrectPageNumber);
        }
        Ok(())
    }

    /// Returns the offset on database file where a Page start given a page number.
    fn offset(&self, page: PageNumber) -> u64 {
        // Start reading pages after pager header; pages start reading at 0.
        (HEADER_SIZE as u32 + page - 1) as u64 * PAGE_SIZE as u64
    }

    /// Check if file buffer is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.file.metadata()?.len() == 0)
    }

    /// Check if the header data is valid on disk.
    fn validate_header(&mut self) -> Result<()> {
        let header = self.read_header()?;

        // TODO: This is right? Seems not.
        if header.magic != MAGIC_BYTES.clone() {
            bail!(Error::CorruptedFile);
        }

        Ok(())
    }

    /// Initialize the default header values.
    fn initialize_header(&mut self) -> Result<()> {
        Ok(self.write_header(&Header::default())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_first_page_not_override_header() -> Result<()> {
        let mut pager = open_test_pager()?;
        let page_number = pager.allocate_page()?;
        let mem_page = [1; PAGE_SIZE];
        pager.write_page(page_number, &mem_page)?;

        let mut page = [0; PAGE_SIZE];
        pager.read_page(page_number, &mut page)?;

        assert_eq!(pager.read_header()?, Header::default());
        assert_eq!(mem_page, page);

        Ok(())
    }

    #[test]
    fn test_open_existed_database_file() -> Result<()> {
        let file = NamedTempFile::new()?;
        {
            // Open empty database file and create a page.
            let mut pager = Pager::open(file.path())?;
            let page_number = pager.allocate_page()?;
            let page_data = [0; PAGE_SIZE];
            pager.write_page(page_number, &page_data)?;
        }

        // Open an already existed database file and create a new page.
        let mut pager = Pager::open(file.path())?;
        let page_number = pager.allocate_page()?;
        let page_data = [0; PAGE_SIZE];
        pager.write_page(page_number, &page_data)?;

        assert_eq!(2, pager.size()?);
        Ok(())
    }

    #[test]
    fn test_pager_size() -> Result<()> {
        let mut pager = open_test_pager()?;
        let total_pages = 20;

        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page()?;
            let page_data = [i; PAGE_SIZE];
            pager.write_page(page_number, &page_data)?;
        }

        assert_eq!(total_pages as u32, pager.size()?);

        Ok(())
    }

    #[test]
    fn test_write_read_pages() -> Result<()> {
        let mut pager = open_test_pager()?;

        let total_pages = 20;

        // Test creating and reading multiple pages to assert
        // that the pager read the correct offset.
        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page()?;
            let page_data = [i; PAGE_SIZE];
            pager.write_page(page_number, &page_data)?;

            let mut page = [0; PAGE_SIZE];
            pager.read_page(page_number, &mut page)?;

            assert_eq!(page_data, page);
        }

        Ok(())
    }

    #[test]
    fn test_read_invalid_page() -> Result<()> {
        let mut pager = open_test_pager()?;
        let mut page = [0; PAGE_SIZE];
        let result = pager.read_page(1, &mut page);

        let err = result.unwrap_err();
        assert_eq!(Error::IncorrectPageNumber, err.downcast::<Error>().unwrap());
        Ok(())
    }

    #[test]
    fn test_read_corrupted_header() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        file.write(&[0; HEADER_SIZE])?;
        let result = Pager::open(file.path());

        let err = result.unwrap_err();
        assert_eq!(Error::CorruptedFile, err.downcast::<Error>().unwrap());
        Ok(())
    }

    #[test]
    fn test_open_new_pager() -> Result<()> {
        let mut pager = open_test_pager()?;
        let header = pager.read_header()?;
        assert_eq!(header, Header::default());
        Ok(())
    }

    fn open_test_pager() -> Result<Pager> {
        let file = NamedTempFile::new()?;
        Pager::open(file.path())
    }
}
