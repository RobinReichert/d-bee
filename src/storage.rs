#![allow(unused)]

pub mod file_management {

    use std::{fs::{self, create_dir_all, metadata, remove_dir_all, remove_file, File, OpenOptions},  io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write}, path::PathBuf};
    use dirs::home_dir;

    pub fn get_base_path() -> PathBuf {
        return home_dir().unwrap().join(".d-bee");
    }

    pub fn create_dir(path : &PathBuf) -> Result<()> {
        return create_dir_all(path);
    }

    pub fn delete_dir(path : &PathBuf) -> Result<()> {
        return remove_dir_all(path);
    }

    pub fn create_file(path : &PathBuf) -> Result<File> {
        File::create_new(path)
    }

    pub fn delete_file(path : &PathBuf) -> Result<()> {
        return remove_file(path);
    }

    pub fn get_size(path : &PathBuf) -> Result<u64> {
        return Ok(metadata(path)?.len());
    }

    pub trait FileHandler {
        fn get_path(&self) -> &PathBuf;
        fn read_at(&self, at : usize, length : usize) -> Result<Vec<u8>>;
        fn write_at(&self, at : usize, data : Vec<u8>) -> Result<()>;
    }

    pub struct SimpleFileHandler {
        path : PathBuf,
    }

    impl SimpleFileHandler {
        pub fn new(path : PathBuf) -> Result<SimpleFileHandler> {
            if !path.is_file() {
                return Err(Error::new(ErrorKind::NotFound, "the path passed is not a file or does not have right permissions"));
            }
            return Ok(SimpleFileHandler {path});
        }
    }

    impl FileHandler for SimpleFileHandler {

        fn get_path(&self) -> &PathBuf {
            return &self.path;
        }

        fn read_at(&self, at : usize, length : usize) -> Result<Vec<u8>> {
            let mut file = OpenOptions::new().read(true).open(&self.path)?;
            file.seek(SeekFrom::Start(at as u64))?;
            let mut buffer = vec![0; length];
            match file.read_exact(&mut buffer) {
                Ok(()) => Ok(buffer),
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => Ok(buffer),
                Err(e) => Err(e)
            }
        }

        fn write_at(&self, at : usize, data : Vec<u8>) -> Result<()> {
            let mut file = OpenOptions::new().write(true).open(&self.path)?;
            file.seek(SeekFrom::Start(at as u64))?;
            return file.write_all(&data);
        }

    }

#[cfg(test)]
    mod tests {

        use super::*;

        #[test]
        fn create_and_delete_directory_test() {
            let dir_path = get_base_path().join("test_dir");
            create_dir(&dir_path).unwrap();
            assert!(dir_path.is_dir(), "Directory was not created");
            delete_dir(&dir_path).unwrap();
            assert!(!dir_path.exists(), "Directory was not deleted");
        }

        #[test]
        fn create_and_delete_file_test() {
            create_dir(&get_base_path());
            let file_path = get_base_path().join("create_and_delete_file.test");
            create_file(&file_path).unwrap();
            assert!(file_path.is_file(), "File was not created");
            delete_file(&file_path).unwrap();
            assert!(!file_path.exists(), "File was not deleted");
        }

        #[test]
        fn write_and_read_test() {
            create_dir(&get_base_path());
            let file_path = get_base_path().join("write_and_read.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"hello world".to_vec();
            handler.write_at(0, data.clone()).unwrap();
            let read_data = handler.read_at(0, data.len()).unwrap();
            assert_eq!(data, read_data, "Data read does not match data written");
            delete_file(&file_path).unwrap();
        }

        #[test]
        fn file_not_found_test() {
            let invalid_path = get_base_path().join("nonexistent_file.test");
            let result = SimpleFileHandler::new(invalid_path.clone());
            assert!(result.is_err(), "Expected error when initializing handler with non-existent file");
        }

        #[test]
        fn read_partial_data_test() {
            create_dir(&get_base_path());
            let file_path = get_base_path().join("read_partial_data.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"hello world".to_vec();
            handler.write_at(0, data.clone()).unwrap();
            let read_data = handler.read_at(0, 5).unwrap(); // Read only "hello"
            assert_eq!(read_data, b"hello", "Partial read does not match expected data");
            delete_file(&file_path).unwrap();
        }

        #[test]
        fn write_beyond_eof_test() {
            let file_path = get_base_path().join("write_beyond_eof.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"beyond eof".to_vec();
            handler.write_at(100, data.clone()).unwrap();
            let read_data = handler.read_at(100, data.len()).unwrap();
            assert_eq!(read_data, data, "Data written beyond EOF does not match expected data");
            delete_file(&file_path).unwrap();
        }
    }

}

pub mod page_management {

    use std::{
        io::{Error, ErrorKind, Result}, 
        path::PathBuf,
        fmt::{self, Display, Formatter}
    };
    use super::file_management::{
        self, 
        FileHandler, 
        SimpleFileHandler
    };
    use crate::bubble::Bubble;

    const PAGE_SIZE : usize = 4096;
    const HEAD_SIZE : usize = 8;

    pub trait PageHandler : Display {
        fn find_fitting_page(&self, size : usize) -> Result<Option<PageHeader>>;
        fn is_page(&self, id : usize) -> Result<Option<PageHeader>>;
        fn alloc_page(&self) -> Result<PageHeader>;
        fn dealloc_page(&self, page : PageHeader) -> Result<()>;
        fn read_page(&self, page : &PageHeader) -> Result<Vec<u8>>;
        fn write_page(&self, page : PageHeader, data : Vec<u8>, size : usize) -> Result<()>;
        fn iterate_pages(&self, f : Box<dyn Fn(Vec<u8>) -> bool>) -> Result<()>; 
    }

    #[derive(Clone)]
    pub struct  PageHeader {
        pub id : usize,
        pub used : usize,
        next : Option<usize>,
        header_page_id : Option<usize>,
        header_offset : Option<usize>,
        previous_page_id : Option<usize>,
    }

    impl PageHeader {
        fn new(id : usize, next : Option<usize>, used : usize, header_page_id : Option<usize>, header_offset : Option<usize>, previous_page_id : Option<usize>) -> PageHeader {
            return PageHeader{id, used,next, header_page_id, header_offset, previous_page_id};
        }

        fn get_size() -> usize {
            return 24;
        }
    }

    pub mod simple {

        use super::*;

        pub struct SimplePageHandler {
            file_handler : Box<dyn FileHandler>
        }

        impl From<Vec<u8>> for PageHeader {
            fn from(value: Vec<u8>) -> Self {
                let id = usize::from_le_bytes(value[0..8].try_into().unwrap());
                let next = usize::from_le_bytes(value[8..16].try_into().unwrap());
                let used = usize::from_le_bytes(value[16..24].try_into().unwrap());
                return PageHeader {id, used, next: if next == 0 {None} else {Some(next)}, header_page_id: None, header_offset: None, previous_page_id: None};

            }
        }

        impl Into<Vec<u8>> for PageHeader {
            fn into(self) -> Vec<u8> {
                let mut buffer = Vec::new();
                buffer.extend(&self.id.to_le_bytes());
                buffer.extend(&self.next.unwrap_or(0).to_le_bytes());
                buffer.extend(&self.used.to_le_bytes());
                return buffer;
            }
        }

        impl Display for PageHeader {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                return write!(f, "id: {}, used: {}, next {}", self.id, self.used, self.next.map_or("none".to_string(), |n| n.to_string()));
            }
        }

        impl SimplePageHandler {
            pub fn new(page_path : PathBuf) -> Result<SimplePageHandler> {
                file_management::create_file(&page_path);                        
                let file_handler = Box::new(SimpleFileHandler::new(page_path)?);
                let page_handler = SimplePageHandler { file_handler };
                if file_management::get_size(page_handler.file_handler.get_path())? < 32 { 
                    page_handler.file_handler.write_at(0, 1_usize.to_le_bytes().to_vec());
                    let first_header = PageHeader::new(0, None, PageHeader::get_size(), None, None, None);
                    page_handler.file_handler.write_at(8, first_header.into());
                }
                return Ok(page_handler);
            }

            fn push_free(&self, id : usize) -> Result<()> {
                let next_bytes : Vec<u8> = self.file_handler.read_at(0, 8)?;
                self.file_handler.write_at(0, id.to_le_bytes().to_vec())?;
                self.file_handler.write_at(SimplePageHandler::calculate_page_start(id), next_bytes)?;
                return Ok(());
            }

            fn pop_free(&self) -> Result<usize> {
                let first_page : usize = usize::from_le_bytes(self.file_handler.read_at(0, 8)?.try_into().unwrap());
                let second_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(first_page), 8)?;
                if second_page_bytes != vec![0, 0, 0, 0, 0, 0, 0, 0] {
                    self.file_handler.write_at(0, second_page_bytes)?;
                }else{
                    self.file_handler.write_at(0, (first_page + 1).to_le_bytes().to_vec())?;
                }
                return Ok(first_page);
            }

            fn calculate_page_start(id : usize) -> usize {
                return id * PAGE_SIZE + HEAD_SIZE;  
            }

            ///Iterates over all headers once until true is returned from f
            fn iterate_headers<F>(&self, mut f : F) -> Result<()> where F : FnMut(PageHeader) -> Result<bool> {
                let mut current_page_id : usize = 0;
                let mut previous_page_id = current_page_id;
                loop {
                    let current_header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(current_page_id), PAGE_SIZE)?;
                    let mut  current_header_offset : usize = 1;
                    let own_header = PageHeader::from(current_header_page_bytes[0..PageHeader::get_size()].to_vec());
                    for current_header_offset in (PageHeader::get_size()..own_header.used).step_by(PageHeader::get_size()) {
                        if let Some(header_bytes) = current_header_page_bytes.get(current_header_offset..current_header_offset + PageHeader::get_size()) {
                            let mut current_header = PageHeader::from(header_bytes.to_vec());
                            current_header.header_page_id = Some(current_page_id);
                            current_header.header_offset = Some(current_header_offset);
                            current_header.previous_page_id = Some(previous_page_id);
                            if f(current_header)? {
                                return Ok(());
                            }
                        }else{
                            break;
                        }
                    }
                    if let Some(next_page_id) = own_header.next {
                        previous_page_id = current_page_id;
                        current_page_id = next_page_id;
                    }else{
                        break;
                    }
                }
                return Ok(());
            }

        }

        impl Display for SimplePageHandler {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let width = 50;
                let mut bubble = Bubble::new(vec![4, width]);
                let first_page : usize = usize::from_le_bytes(self.file_handler.read_at(0, 8).unwrap().try_into().unwrap());
                bubble.add_line(vec!["head", &format!("next free page at: {}", first_page.to_string())]);
                'outer:
                    for i in 0..10 {
                        let mut j : usize = 0;
                        bubble.add_divider();
                        //Check if page is a header page and if so show headers
                        loop{
                            let header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(j), PAGE_SIZE).unwrap();
                            let page_header = PageHeader::from(header_page_bytes[0..PageHeader::get_size()].to_vec());
                            if page_header.id == i {
                                for n in (0..page_header.used).step_by(PageHeader::get_size()) {
                                    let m :usize = n + PageHeader::get_size();
                                    if let Some(header_bytes) = header_page_bytes.get(n..m) {
                                        let mut header = PageHeader::from(header_bytes.to_vec());
                                        bubble.add_line(vec![&i.to_string(), &header.to_string()]);
                                    }
                                }
                                continue 'outer;
                            }
                            if let Some(next) = page_header.next {
                                j = next;
                            }else{
                                break;
                            }
                        }
                        //Check if page is in the free list
                        j = usize::from_le_bytes(self.file_handler.read_at(0, 8).unwrap().try_into().unwrap());
                        loop {
                            let next : usize = usize::from_le_bytes(self.file_handler.read_at(SimplePageHandler::calculate_page_start(j), 8).unwrap().try_into().unwrap());
                            if next == 0 {
                                break;
                            }
                            if j == i {
                                bubble.add_line(vec![&i.to_string(), &next.to_string()]);
                                continue 'outer;
                            }
                            j = next;
                        }
                        //Write used space
                        let mut allocated = false;
                        self.iterate_headers(|h| {
                            if i == h.id {
                                let space = h.used * width / PAGE_SIZE;
                                let mut space_representation = String::new();
                                for _ in 0..space {
                                    space_representation.push_str("#");
                                }
                                for _ in space..width {
                                    space_representation.push_str(".");
                                }
                                bubble.add_line(vec![&i.to_string(), &space_representation]);
                                allocated = true;
                                return Ok(true);
                            }
                            return Ok(false);
                        });
                        if !allocated {
                            bubble.add_line(vec![&i.to_string(), ""]);
                        }
                    }
                write!(f, "{}", bubble)
            }
        }

        impl PageHandler for SimplePageHandler {

            fn find_fitting_page(&self, size : usize) -> Result<Option<PageHeader>> {
                let mut header : Option<PageHeader> = None;
                self.iterate_headers(|current_header| {
                    if PAGE_SIZE - current_header.used >= size {
                        header = Some(current_header);
                        return Ok(true);
                    }
                    return Ok(false);
                })?;
                return Ok(header);
            }

            fn is_page(&self, id : usize) -> Result<Option<PageHeader>> {
                let mut header : Option<PageHeader> = None;
                self.iterate_headers(|current_header| {
                    if current_header.id == id {
                        header = Some(current_header);
                        return Ok(true);
                    }
                    return Ok(false);
                })?;
                return Ok(header);
            }

            fn alloc_page(&self) -> Result<PageHeader> {
                let mut current_header_page_id : usize = 0;
                let mut new_page_id = self.pop_free()?;
                loop {
                    let mut current_header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(current_header_page_id), PAGE_SIZE)?;
                    let mut own_header = PageHeader::from(current_header_page_bytes[0..PageHeader::get_size()].to_vec());
                    if PAGE_SIZE - own_header.used > PageHeader::get_size() {
                        //Add new header to the header page
                        let new_header = PageHeader::new(new_page_id, None, 0, Some(own_header.id), Some(own_header.used), None);
                        let new_header_bytes : Vec<u8> = new_header.clone().into();
                        current_header_page_bytes[own_header.used..own_header.used + PageHeader::get_size()].copy_from_slice(&new_header_bytes);
                        //Increase used value
                        own_header.used += PageHeader::get_size();
                        current_header_page_bytes[..PageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(current_header_page_id), current_header_page_bytes)?;
                        return Ok(new_header);
                    }
                    if let Some(next_header_page_id) = own_header.next {
                        //In case one header page did not have enough space for another header and
                        //another one exists already the loop gets repeated with the next header page
                        current_header_page_id = next_header_page_id;     
                    }else{
                        //In case one page is full and no next was created a new one is appended to the
                        //previous page.
                        own_header.next = Some(new_page_id);
                        let own_header_bytes : Vec<u8> = own_header.clone().into();
                        current_header_page_bytes[..PageHeader::get_size()].copy_from_slice(&own_header_bytes); 
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(current_header_page_id), current_header_page_bytes);
                        let new_own_header = PageHeader::new(new_page_id, None, PageHeader::get_size(), None, None, Some(own_header.id));
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(new_page_id), new_own_header.into());
                        current_header_page_id = new_page_id;
                        new_page_id = self.pop_free()?;
                    }
                }
                return Err(Error::new(ErrorKind::Other, "unexpected error"));
            }

            fn dealloc_page(&self, page_header : PageHeader) -> Result<()> {
                if let Some(next_page_header_id) = page_header.next {
                    self.dealloc_page(self.is_page(next_page_header_id)?.ok_or(ErrorKind::InvalidInput)?);
                }
                let header_page_id = page_header.header_page_id.ok_or_else(||{Error::new(ErrorKind::InvalidInput, "header did not contain header_page_id")})?;
                let mut header_page_bytes : Vec<u8> = self.file_handler.read_at(SimplePageHandler::calculate_page_start(header_page_id), PAGE_SIZE)?;
                //Remove header from header page_header
                let header_offset : usize = page_header.header_offset.ok_or(ErrorKind::InvalidInput)?;
                header_page_bytes.drain(header_offset..(header_offset + PageHeader::get_size())); 
                //Decrease used value
                let mut own_header = PageHeader::from(header_page_bytes[..PageHeader::get_size()].to_vec());
                own_header.used -= PageHeader::get_size();
                //If a header page_header is empty it gets removed
                if own_header.used <= PageHeader::get_size() && page_header.header_page_id.unwrap() != 0 {
                    let previous_page_id = page_header.previous_page_id.ok_or_else(|| {Error::new(ErrorKind::InvalidInput, "header did not contain previous_page_id")})?;
                    let previous_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(previous_page_id), PAGE_SIZE)?;
                    let mut previous_page_header = PageHeader::from(previous_page_bytes[..PageHeader::get_size()].to_vec());
                    previous_page_header.next = own_header.next;
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(previous_page_id), previous_page_header.into());
                }else{
                    header_page_bytes[..PageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(page_header.header_page_id.unwrap()), header_page_bytes)?;
                }
                //Add page_header to free list
                self.push_free(page_header.id);
                return Ok(());
            }

            fn read_page(&self, page_header : &PageHeader) -> Result<Vec<u8>> {
                    return self.file_handler.read_at(SimplePageHandler::calculate_page_start(page_header.id), PAGE_SIZE);
                return Err(Error::new(ErrorKind::InvalidInput, "wrong header type"));
            }

            fn write_page(&self, page_header : PageHeader, data : Vec<u8>, size : usize) -> Result<()> {
                if data.len() > PAGE_SIZE {
                    return Err(Error::new(ErrorKind::ArgumentListTooLong, "data is to big to write into one page"));
                }
                let header_page_id = page_header.header_page_id.ok_or(ErrorKind::InvalidInput)?;
                let mut header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(header_page_id), PAGE_SIZE)?;
                let header_offset : usize = page_header.header_offset.ok_or_else(|| {Error::new(ErrorKind::InvalidInput, "header did not have a header_offset")})?;
                let header_bytes = header_page_bytes.get(header_offset..(header_offset + PageHeader::get_size())).ok_or_else(|| {Error::new(ErrorKind::Other, "unexpected error")})?;
                let mut own_header = PageHeader::from(header_bytes.to_vec());
                if own_header.id == page_header.id {
                    own_header.used = size;
                    header_page_bytes[header_offset..(header_offset + PageHeader::get_size())].copy_from_slice(&Into::<Vec<u8>>::into(own_header));
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(page_header.id), data)?;
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(page_header.header_page_id.unwrap()), header_page_bytes)?;
                    return Ok(());
                }
                return Err(Error::new(ErrorKind::InvalidInput, "wrong header type"));
            }

            fn iterate_pages(&self, mut f : Box<dyn Fn(Vec<u8>) -> bool>) -> Result<()> {
                self.iterate_headers(|h| {
                    return Ok(f(self.read_page(&h)?));
                })?;
                return Ok(());
            }
        }

        #[cfg(test)]
        mod test {

            use super::*;

            #[test]
            fn read_write_test() {
                let path = file_management::get_base_path().join("read_write.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let data = b"Hello, Page!".to_vec();
                handler.write_page(handler.alloc_page().unwrap(), data.clone(), data.len()).unwrap();
                let mut read_data = handler.read_page(&handler.is_page(1).unwrap().unwrap()).unwrap();
                read_data.truncate(data.len());
                assert_eq!(data, read_data);
            }

            #[test]
            fn find_fitting_page_test() {
                let path = file_management::get_base_path().join("find_fitting_page.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let page1 = handler.alloc_page().unwrap();
                let page2 = handler.alloc_page().unwrap();
                handler.write_page(page1, vec![0; PAGE_SIZE - 10], PAGE_SIZE - 10).unwrap();
                let fitting_page = handler.find_fitting_page(20).unwrap();
                assert_eq!(page2.id, fitting_page.unwrap().id);
            }

            #[test]
            fn dont_find_fitting_page_test() {
                let path = file_management::get_base_path().join("dont_find_fitting_page.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let page1 = handler.alloc_page().unwrap();
                handler.write_page(page1, vec![0; PAGE_SIZE - 10], PAGE_SIZE - 10).unwrap();
                let fitting_page = handler.find_fitting_page(90).unwrap();
                assert!(matches!(fitting_page, None), "expected none but found some");
            }

            #[test]
            fn invalid_dealloc_test() {
                let path = file_management::get_base_path().join("invalid_dealloc.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path.clone()).unwrap());
                let result = handler.dealloc_page(PageHeader::new(999, None, 0, None, None, None));
                assert!(result.is_err(), "Expected error when deallocating non-existent page");
            }

            #[test]
            fn free_list_integrity_test() {
                let path = file_management::get_base_path().join("free_list_integrity.test");
                file_management::delete_file(&path);
                let handler = Box::new(SimplePageHandler::new(path.clone()).unwrap());
                let page1 = handler.alloc_page().unwrap();
                let page2 = handler.alloc_page().unwrap();
                let id1 = page1.id;
                let id2 = page2.id;
                handler.dealloc_page(page1).unwrap();
                handler.dealloc_page(page2).unwrap();
                let page3 = handler.alloc_page().unwrap();
                assert_eq!(page3.id, id2); // Reuse from free list
                let page4 = handler.alloc_page().unwrap();
                assert_eq!(page4.id, id1); // Reuse from free list
            }

            #[test]
            fn header_conversion_test() {
                let original_header = PageHeader::new(1, Some(2), 50, None, None, None);
                let header_bytes: Vec<u8> = original_header.clone().into();
                let reconstructed_header = PageHeader::from( header_bytes);
                assert_eq!(original_header.id, reconstructed_header.id);
                assert_eq!(original_header.next, reconstructed_header.next);
                assert_eq!(original_header.used, reconstructed_header.used);
            }

        }

    }

}

pub mod table_management {

    use super::{file_management, page_management::{PageHandler, simple::{SimplePageHandler}}};
    use std::{
        io::{self, Result},
        path::PathBuf,
        fmt::{self, Display, Formatter}
    };
    pub trait TableHandler {
        fn insert_row(&self, row : Row) -> Result<()>;
        fn select_row(&self, predicate : Predicate) -> Result<Cursor>;
        fn delete_row(&self, predicate : Predicate) -> Result<()>;
    }
    
    #[derive(Clone)]
    pub enum Value {
        Text(String),
        Number(usize),
    }

    pub enum ByteValue {
        Text(Vec<u8>),
        Number(Vec<u8>),
    }

    pub struct Row {
        cols : Vec<Value>,
    }

    pub enum Operator {
        Equals,
        Less,
        LessOrEqual,
        Bigger,
        BiggerOrEqual,
    }

    pub struct Predicate {
        column : String,
        operator : Operator,
        value : Value,
    }

    pub struct Cursor {
    }

    impl Display for Row {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            let mut result  = String::new(); 
            for col in &self.cols {
                result.push_str(&col.to_string());
            }
            return write!(f, "{}", result);
        }
    }

    impl Into<Vec<u8>> for Value {
        fn into(self) -> Vec<u8> {
            match self { 
                Self::Text(val) => {val.into_bytes().to_vec()},
                Self::Number(val) => {val.to_le_bytes().to_vec()},
            }
        }
    }

    impl From<ByteValue> for Value {
        fn from(value: ByteValue) -> Self {
            match value {
                ByteValue::Text(bytes) => {Value::Text(String::from_utf8(bytes).unwrap())},
                ByteValue::Number(bytes) => {Value::Number(usize::from_le_bytes(bytes.try_into().unwrap()))},
            }
       }
    }

    impl Display for Value {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            match self { 
                Self::Text(val) => write!(f, "{}", val),
                Self::Number(val) => write!(f, "{}", val),
            }
        }
    }

    pub mod simple {

        use super::*;

        pub struct SimpleTableHandler {
            page_handler : Box<dyn PageHandler>,
            col_types : Vec<Value>,
            col_names : Vec<String>,
        }

        impl Into<Vec<u8>> for Row {
            fn into(self) -> Vec<u8> {
                let mut buffer = Vec::new();
                let offset_size = (usize ::BITS / 8) as usize;
                buffer.resize(self.cols.len() * offset_size, 0); 
                let mut offset_cumulative : usize = self.cols.len() * offset_size;
                for (index, col) in self.cols.into_iter().enumerate() {
                    let mut col_bytes : Vec<u8> = col.into();
                    offset_cumulative += col_bytes.len();
                    buffer[index * offset_size..(index + 1) * offset_size].copy_from_slice(&offset_cumulative.to_le_bytes().to_vec());
                    buffer.append(&mut col_bytes);
                }
                return buffer;
            }
        }

        fn row_from_bytes(bytes : Vec<u8>, col_types : Vec<Value>) -> Row {
            let offset_size = (usize ::BITS / 8) as usize;
            let mut last_col_offset = col_types.len() * offset_size;
            let mut row = Row {cols : Vec::new()};
            for (index, col) in col_types.iter().enumerate() {
                let col_offset = usize::from_le_bytes(bytes[(index * offset_size)..((index + 1) * offset_size)].try_into().unwrap());
                let col_bytes : Vec<u8> = bytes[last_col_offset..col_offset].try_into().unwrap();
                let val = Value::from(match col {
                    Value::Text(_) => ByteValue::Text(col_bytes),
                    Value::Number(_) => ByteValue::Number(col_bytes),
                });
                row.cols.push(val);
                last_col_offset = col_offset;
            }
            return row;
        }

        impl SimpleTableHandler {
            fn new(table_path : PathBuf, col_types : Vec<Value>, col_names : Vec<String>) -> Result<SimpleTableHandler> {
                let page_handler = Box::new(SimplePageHandler::new(table_path)?);
                return Ok(SimpleTableHandler {page_handler, col_types, col_names});
            }
        }

        impl TableHandler for SimpleTableHandler {
            fn insert_row(&self, row : Row) -> Result<()> {
                let mut row_bytes : Vec<u8> = row.into();
                let row_size = row_bytes.len();
                let ptr_size = (usize::BITS / 8) as usize;
                let page_header = match self.page_handler.find_fitting_page(row_size + ptr_size)? {
                    Some(p) => p,
                    None => self.page_handler.alloc_page()?,
                };
                let used = page_header.used + row_size + ptr_size;
                let mut page = self.page_handler.read_page(&page_header)?; 
                let ptr_count = usize::from_le_bytes(page[0..ptr_size].try_into().unwrap());
                let data_offset = usize::from_le_bytes(page[(ptr_count * ptr_size)..((ptr_count + 1) * ptr_size)].try_into().unwrap()); 
                page[0..ptr_size].copy_from_slice(&(ptr_count+1).to_le_bytes().to_vec());
                page[((ptr_count + 1) * ptr_size)..((ptr_count + 2) * ptr_size)].copy_from_slice(&(data_offset + row_size).to_le_bytes().to_vec());
                let start : usize = page.len() - (data_offset + row_size);
                let end : usize = page.len() - data_offset;
                page[start..end].copy_from_slice(&row_bytes);
                self.page_handler.write_page(page_header, page, used);
                return Ok(());
            }

            fn delete_row(&self, predicate : Predicate) -> Result<()> {
                todo!();    
            }

            fn select_row(&self, predicate : Predicate) -> Result<Cursor> {
                let col_types = self.col_types.clone();
                self.page_handler.iterate_pages(Box::new(move |page|{
                    let ptr_size = (usize::BITS / 8) as usize;
                    let ptr_count = usize::from_le_bytes(page[0..ptr_size].try_into().unwrap());
                    let mut last_data_offset : usize = 0;
                    for ptr_index in 0..ptr_count {
                        let data_offset = usize::from_le_bytes(page[((ptr_index + 1) * ptr_size)..((ptr_index + 2) * ptr_size)].try_into().unwrap()); 
                        let start : usize = page.len() - data_offset;
                        let end : usize = page.len() - last_data_offset;
                        let row_bytes : Vec<u8> = page[start..end].try_into().unwrap();
                        let row : Row = row_from_bytes(row_bytes, col_types.clone());
                        println!("row: {}", row);
                        last_data_offset = data_offset;
                    }
                    return false;
                }));
                return Ok(Cursor{});
            }
        }

        #[cfg(test)]
        mod test {

            use super::*;
            use super::file_management::{
                self, 
                FileHandler, 
                SimpleFileHandler
            };

            #[test]
            fn print_test() {
                let path = file_management::get_base_path().join("print.test");
                file_management::delete_file(&path);
                let handler = Box::new(SimpleTableHandler::new(path.clone(), vec![Value::Text("".to_string()), Value::Number(0)], vec!["test".to_string()]).unwrap());
                handler.insert_row(Row { cols: vec![Value::Text("hallo".to_string()), Value::Number(19)]});
                handler.insert_row(Row { cols: vec![Value::Text("welt".to_string()), Value::Number(2012873284237)]});
                handler.insert_row(Row { cols: vec![Value::Text("felix stinkt die bahn ist scheiße kein plan was ich sonst noch schreiben soll. aber ich brauche genügend character um eine seite zu ueberfuellen.".to_string()), Value::Number(1)]});
                handler.select_row(Predicate{ column: "".to_string(), operator: Operator::Less, value: Value::Text("hallo".to_string())});
            }
        }
    }
}
