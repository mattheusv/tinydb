use log::debug;
use std::fs::{File, OpenOptions};
use std::io::{
    self,
    prelude::{Read, Write},
    Seek, SeekFrom,
};
use std::path::Path;

/// Represents the tinydb header size.
const HEADER_SIZE: usize = 100;

/// Represents the size that a Page can have on database file.
pub const PAGE_SIZE: usize = 4096 * 4; // 8 Kb

/// Header is a type that represents the array of bytes
/// containing the header data from database file.
pub type Header = [u8; HEADER_SIZE];

/// PageData is a type that represents the array of bytes
/// of some page in database.
pub type PageData = [u8; PAGE_SIZE];

/// Represents the type of PageNumber.
pub type PageNumber = u32;

/// Represents errors that pager can have.
#[derive(Debug, PartialEq)]
pub enum Error {
    /// Represents an invalid page number on database file.
    IncorrectPageNumber,

    /// Represents I/O related errors.
    IO(io::ErrorKind),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err.kind())
    }
}

/// Represents a in-memory copy of page.
#[derive(Debug, PartialEq)]
pub struct MemPage {
    /// Represents the number of page on database file.
    pub number: PageNumber,

    /// Represents the actual bytes data from page.
    pub data: PageData,
}

/// Pager handle all read/write operations on database file.
///
/// If you want to modify the file, you need to modify the page returned by
/// the pager and instruct the pager to write it back to disk.
///
/// The Pager is very simple and always creates an in-memory copy of any page
/// that is read (even if that page has already been read before).
/// More specifically, pages are read into a MemPage structure.
pub struct Pager {
    file: File,
    total_pages: u32,
}

impl Pager {
    /// Opens a file for paged access.
    pub fn open(filename: &Path) -> Result<Self, Error> {
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
        Ok(pager)
    }

    /// Read a page from file.  This pager reads a page from the file,
    /// and creates an in-memory copy in a MemPage struct. Any changes
    /// done to a MemPage will not be effective until you call
    /// write_0age with that MemPage.
    pub fn read_page(&mut self, page: PageNumber) -> Result<MemPage, Error> {
        self.validate_page(page)?;
        self.file.seek(SeekFrom::Start(self.offset(page)))?;
        let mut data: PageData = [0; PAGE_SIZE];
        let count = self.file.read(&mut data)?;
        debug!("Read {} bytes from page {}", count, page);
        Ok(MemPage { data, number: page })
    }

    /// Write a page to file. This pager writes the in-memory copy of a
    /// page (stored in a MemPage struct) back to disk.
    pub fn write_page(&mut self, page: &MemPage) -> Result<(), Error> {
        self.validate_page(page.number)?;
        self.file.seek(SeekFrom::Start(self.offset(page.number)))?;
        let count = self.file.write(&page.data)?;
        debug!("Wrote {} bytes to page {}", count, page.number);
        Ok(())
    }

    /// Allocate an extra page on the file and returns the page number
    pub fn allocate_page(&mut self) -> u32 {
        // We simply increment the page number counter.
        // read_page and write_page take care of the rest.
        self.total_pages += 1;
        self.total_pages
    }

    /// Reads the header of database file and returns it in a byte array.
    /// Note that this function can be called even if the page size is unknown,
    /// since the chidb header always occupies the first 100 bytes of the file.
    pub fn read_header(&mut self) -> Result<Header, Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        self.file.read(&mut header)?;
        Ok(header)
    }

    /// Write the header on database file. Note that the write_header function will
    /// always override the current header data if exists.
    pub fn write_header(&mut self, header: &Header) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write(header)?;
        Ok(())
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32, Error> {
        let len = self.file.metadata()?.len();
        if len == 0 {
            return Ok(0);
        }
        Ok((len as u32 / PAGE_SIZE as u32) - HEADER_SIZE as u32)
    }

    /// Check if a pager number is valid to this database file buffer.
    fn validate_page(&self, page: PageNumber) -> Result<(), Error> {
        if page > self.total_pages || page <= 0 {
            return Err(Error::IncorrectPageNumber);
        }
        Ok(())
    }

    /// Returns the offset on database file where a Page start given a page number.
    fn offset(&self, page: PageNumber) -> u64 {
        // Start reading pages after pager header; pages start reading at 0.
        (HEADER_SIZE as u32 + page - 1) as u64 * PAGE_SIZE as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_first_page_not_override_header() -> Result<(), Error> {
        let mut pager = open_test_pager()?;
        let header = [10; HEADER_SIZE];
        pager.write_header(&header)?;

        let page_number = pager.allocate_page();
        let mem_page = MemPage {
            data: [1; PAGE_SIZE],
            number: page_number,
        };
        pager.write_page(&mem_page)?;

        assert_eq!(header, pager.read_header()?);
        assert_eq!(mem_page, pager.read_page(page_number)?);

        Ok(())
    }

    #[test]
    fn test_open_existed_database_file() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        {
            // Open empty database file and create a page.
            let mut pager = Pager::open(file.path())?;
            let page_number = pager.allocate_page();
            let page_data: PageData = [0; PAGE_SIZE];
            let mem_page = MemPage {
                number: page_number,
                data: page_data,
            };
            pager.write_page(&mem_page)?;
        }

        // Open an already existed database file and create a new page.
        let mut pager = Pager::open(file.path())?;
        let page_number = pager.allocate_page();
        let page_data: PageData = [0; PAGE_SIZE];
        let mem_page = MemPage {
            number: page_number,
            data: page_data,
        };
        pager.write_page(&mem_page)?;

        assert_eq!(2, pager.size()?);
        Ok(())
    }

    #[test]
    fn test_pager_size() -> Result<(), Error> {
        let mut pager = open_test_pager()?;
        let total_pages = 20;

        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page();
            let page_data: PageData = [i; PAGE_SIZE];
            let mem_page = MemPage {
                number: page_number,
                data: page_data,
            };
            pager.write_page(&mem_page)?;
        }

        assert_eq!(total_pages as u32, pager.size()?);

        Ok(())
    }

    #[test]
    fn test_write_read_pages() -> Result<(), Error> {
        let mut pager = open_test_pager()?;

        let total_pages = 20;

        // Test creating and reading multiple pages to assert
        // that the pager read the correct offset.
        for i in 0..total_pages {
            let page_number: PageNumber = pager.allocate_page();
            let page_data: PageData = [i; PAGE_SIZE];
            let mem_page = MemPage {
                number: page_number,
                data: page_data,
            };
            pager.write_page(&mem_page)?;

            let page = pager.read_page(page_number)?;

            assert_eq!(mem_page, page);
        }

        Ok(())
    }

    #[test]
    fn test_read_invalid_page() -> Result<(), Error> {
        let mut pager = open_test_pager()?;
        let result = pager.read_page(1);
        assert_eq!(Error::IncorrectPageNumber, result.unwrap_err());
        Ok(())
    }

    #[test]
    fn test_read_header() -> Result<(), Error> {
        let mut pager = open_test_pager()?;
        let header = pager.read_header()?;
        assert_eq!(header, [0; HEADER_SIZE], "Expected empty header");
        Ok(())
    }

    #[test]
    fn test_write_header() -> Result<(), Error> {
        let mut pager = open_test_pager()?;

        let header = [1; HEADER_SIZE];

        pager.write_header(&header)?;
        let readed_header = pager.read_header()?;

        assert_eq!(header, readed_header);

        Ok(())
    }

    fn open_test_pager() -> Result<Pager, Error> {
        let file = NamedTempFile::new()?;
        Pager::open(file.path())
    }
}
