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
            let file_path = get_base_path().join("create_and_delete_file.test");
            create_file(&file_path).unwrap();
            assert!(file_path.is_file(), "File was not created");
            delete_file(&file_path).unwrap();
            assert!(!file_path.exists(), "File was not deleted");
        }

        #[test]
        fn write_and_read_test() {
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
        fn read_page(&self, page : PageHeader) -> Result<Vec<u8>>;
        fn write_page(&self, page : PageHeader, data : Vec<u8>) -> Result<()>;
    }

    pub enum PageHeader {
        Simple(simple::SimplePageHeader),
        None()
    }

    impl PageHeader {
        fn get_id(&self) -> usize {
            match self {
                PageHeader::Simple(h) => h.id,
                PageHeader::None() => {panic!()},
            }
        }
    }

    pub mod simple {

        use super::{
            PAGE_SIZE, 
            HEAD_SIZE,
            file_management, 
            FileHandler, 
            SimpleFileHandler, 
            PathBuf, 
            fmt,
            Display, 
            Formatter, 
            Bubble, 
            PageHandler, 
            PageHeader,
            Result, 
            ErrorKind, 
            Error
        };

        pub struct SimplePageHandler {
            file_handler : Box<dyn FileHandler>
        }

        #[derive(Clone)]
        pub struct SimplePageHeader {
            pub id : usize,
            next : Option<usize>,
            used : usize,
            header_page_id : Option<usize>,
            header_offset : Option<usize>,
            previous_page_id : Option<usize>,
        }

        impl SimplePageHeader {
            fn new(id : usize, next : Option<usize>, used : usize, header_page_id : Option<usize>, header_offset : Option<usize>, previous_page_id : Option<usize>) -> SimplePageHeader {
                return SimplePageHeader{id, next, used, header_page_id, header_offset, previous_page_id};
            }

            fn get_size() -> usize {
                return 24;
            }
        }

        impl From<Vec<u8>> for SimplePageHeader {
            fn from(value: Vec<u8>) -> Self {
                let id = usize::from_le_bytes(value[0..8].try_into().unwrap());
                let next = usize::from_le_bytes(value[8..16].try_into().unwrap());
                let used = usize::from_le_bytes(value[16..24].try_into().unwrap());
                return SimplePageHeader {id, next: if next == 0 {None} else {Some(next)}, used, header_page_id: None, header_offset: None, previous_page_id: None};

            }
        }

        impl Into<Vec<u8>> for SimplePageHeader {
            fn into(self) -> Vec<u8> {
                let mut buffer = Vec::new();
                buffer.extend(&self.id.to_le_bytes());
                buffer.extend(&self.next.unwrap_or(0).to_le_bytes());
                buffer.extend(&self.used.to_le_bytes());
                return buffer;
            }
        }

        impl ToString for SimplePageHeader {
            fn to_string(&self) -> String {
                return format!("id: {}, used: {}, next {}", self.id, self.used, self.next.map_or("none".to_string(), |n| n.to_string()));
            }
        }

        impl SimplePageHandler {
            pub fn new(page_path : PathBuf) -> Result<SimplePageHandler> {
                file_management::create_file(&page_path);                        
                let file_handler = Box::new(SimpleFileHandler::new(page_path)?);
                let page_handler = SimplePageHandler { file_handler };
                if file_management::get_size(page_handler.file_handler.get_path())? < 32 { 
                    page_handler.file_handler.write_at(0, 1_usize.to_le_bytes().to_vec());
                    let first_header = SimplePageHeader::new(0, None, SimplePageHeader::get_size(), None, None, None);
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
                    self.file_handler.write_at(0, second_page_bytes);
                }else{
                    self.file_handler.write_at(0, (first_page+1).to_le_bytes().to_vec());
                }
                return Ok(first_page);
            }

            fn calculate_page_start(id : usize) -> usize {
                return id * PAGE_SIZE + HEAD_SIZE;  
            }

            fn iterate_headers<F>(&self, mut f : F) -> Result<()> where F : FnMut(SimplePageHeader) -> bool {
                let mut i : usize = 0;
                let mut prev = i;
                loop {
                    let header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(i), PAGE_SIZE)?;
                    let mut  n : usize = 1;
                    let own_header = SimplePageHeader::from(header_page_bytes[0..SimplePageHeader::get_size()].to_vec());
                    for n in (SimplePageHeader::get_size()..own_header.used).step_by(SimplePageHeader::get_size()) {
                        let m :usize = n + SimplePageHeader::get_size();
                        if let Some(header_bytes) = header_page_bytes.get(n..m) {
                            let mut header = SimplePageHeader::from(header_bytes.to_vec());
                            header.header_page_id = Some(i);
                            header.header_offset = Some(n);
                            header.previous_page_id = Some(prev);
                            if f(header) {
                                return Ok(());
                            }
                        }else{
                            break;
                        }
                    }
                    let header = SimplePageHeader::from(header_page_bytes[0..SimplePageHeader::get_size()].to_vec());
                    if let Some(next) = header.next {
                        prev = i;
                        i = next;
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
                            let page_header = SimplePageHeader::from(header_page_bytes[0..SimplePageHeader::get_size()].to_vec());
                            if page_header.id == i {
                                for n in (0..page_header.used).step_by(SimplePageHeader::get_size()) {
                                    let m :usize = n + SimplePageHeader::get_size();
                                    if let Some(header_bytes) = header_page_bytes.get(n..m) {
                                        let mut header = SimplePageHeader::from(header_bytes.to_vec());
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
                                return true;
                            }
                            return false;
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
                self.iterate_headers(|h| {
                    if PAGE_SIZE - h.used >= size {
                        header = Some(PageHeader::Simple(h));
                        return true;
                    }
                    return false;
                })?;
                return Ok(header);
            }

            fn is_page(&self, id : usize) -> Result<Option<PageHeader>> {
                let mut header : Option<PageHeader> = None;
                self.iterate_headers(|h| {
                    if h.id == id {
                        header = Some(PageHeader::Simple(h));
                        return true;
                    }
                    return false;
                })?;
                return Ok(header);
            }

            fn alloc_page(&self) -> Result<PageHeader> {
                let mut i : usize = 0;
                loop {
                    let new_page_id = self.pop_free()?;
                    let mut page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(i), PAGE_SIZE)?;
                    let mut own_header = SimplePageHeader::from(page_bytes[0..SimplePageHeader::get_size()].to_vec());
                    if PAGE_SIZE - own_header.used > SimplePageHeader::get_size() {
                        //Add new header to the header page
                        let new_header = SimplePageHeader::new(new_page_id, None, 0, Some(own_header.id), Some(own_header.used), None);
                        let new_header_bytes : Vec<u8> = new_header.clone().into();
                        page_bytes[own_header.used..own_header.used + SimplePageHeader::get_size()].copy_from_slice(&new_header_bytes);
                        //Increase used value
                        own_header.used += SimplePageHeader::get_size();
                        page_bytes[..SimplePageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(i), page_bytes)?;
                        return Ok(PageHeader::Simple(new_header));
                    }
                    if let Some(next) = own_header.next {
                        //In case one header page did not have enough space for another header and
                        //another one exists already the loop gets repeated with the next header page
                        i = next;     
                    }else{
                        //In case one page is full and no next was created a new one is appended to the
                        //previous page.
                        own_header.next = Some(new_page_id);
                        let header_bytes : Vec<u8> = own_header.clone().into();
                        page_bytes[..SimplePageHeader::get_size()].copy_from_slice(&header_bytes); 
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(i), page_bytes);
                        let new_own_header = SimplePageHeader::new(new_page_id, None, SimplePageHeader::get_size(), None, None, Some(own_header.id));
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(new_page_id), new_own_header.into());
                        i = new_page_id;
                    }
                }
                return Err(Error::new(ErrorKind::Other, "unexpected error"));
            }

            fn dealloc_page(&self, p : PageHeader) -> Result<()> {
                if let PageHeader::Simple(page) = p {
                    if let Some(next) = page.next {
                        self.dealloc_page(self.is_page(page.next.ok_or(ErrorKind::InvalidInput)?)?.ok_or(ErrorKind::InvalidInput)?);
                    }
                    let mut header_page_bytes : Vec<u8> = self.file_handler.read_at(SimplePageHandler::calculate_page_start(page.header_page_id.ok_or(ErrorKind::InvalidInput)?), PAGE_SIZE)?;
                    //Remove header from header page
                    let offset : usize = page.header_offset.ok_or(ErrorKind::InvalidInput)?;
                    header_page_bytes.drain(offset..(offset+SimplePageHeader::get_size())); 
                    //Decrease used value
                    let mut own_header = SimplePageHeader::from(header_page_bytes[..SimplePageHeader::get_size()].to_vec());
                    own_header.used -= SimplePageHeader::get_size();
                    //If a header page is empty it gets removed
                    if own_header.used <= SimplePageHeader::get_size() && page.header_page_id.unwrap() != 0 {
                        let mut prev_header = SimplePageHeader::from(self.file_handler.read_at(SimplePageHandler::calculate_page_start(page.previous_page_id.ok_or(ErrorKind::InvalidInput)?), PAGE_SIZE)?[..SimplePageHeader::get_size()].to_vec());
                        prev_header.next = own_header.next;
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(page.previous_page_id.unwrap()), prev_header.into());
                    }else{
                        header_page_bytes[..SimplePageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(page.header_page_id.unwrap()), header_page_bytes)?;
                    }
                    //Add page to free list
                    self.push_free(page.id);
                    return Ok(());
                }
                return Err(Error::new(ErrorKind::InvalidInput, "wrong header"));
            }

            fn read_page(&self, p : PageHeader) -> Result<Vec<u8>> {
                if let PageHeader::Simple(page) = p {
                    return self.file_handler.read_at(SimplePageHandler::calculate_page_start(page.id), page.used);
                }

                return Err(Error::new(ErrorKind::InvalidInput, "wrong header"));
            }

            fn write_page(&self, p : PageHeader, data : Vec<u8>) -> Result<()> {
                if data.len() > PAGE_SIZE {
                    return Err(Error::new(ErrorKind::ArgumentListTooLong, "data is to big to write into one page"));
                }
                if let PageHeader::Simple(page) = p {
                    let mut header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(page.header_page_id.ok_or(ErrorKind::InvalidInput)?), PAGE_SIZE)?;
                    let offset : usize = page.header_offset.ok_or(ErrorKind::InvalidInput)?;
                    let header_bytes = header_page_bytes.get(offset..(offset+SimplePageHeader::get_size())).ok_or(ErrorKind::InvalidInput)?;
                    let mut header = SimplePageHeader::from(header_bytes.to_vec());
                    if header.id == page.id {
                        header.used = data.len();
                        header_page_bytes[offset..(offset+SimplePageHeader::get_size())].copy_from_slice(&Into::<Vec<u8>>::into(header));
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(page.id), data)?;
                        self.file_handler.write_at(SimplePageHandler::calculate_page_start(page.header_page_id.unwrap()), header_page_bytes)?;
                        return Ok(());
                    }
                    return Err(Error::new(ErrorKind::InvalidInput, "header_id was wrong"));
                }
                return Err(Error::new(ErrorKind::InvalidInput, "wrong header"));
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
                handler.write_page(handler.alloc_page().unwrap(), data.clone()).unwrap();
                let read_data = handler.read_page(handler.is_page(1).unwrap().unwrap()).unwrap();
                assert_eq!(data, read_data);
            }

            #[test]
            fn find_fitting_page_test() {
                let path = file_management::get_base_path().join("find_fitting_page.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let page1 = handler.alloc_page().unwrap();
                let page2 = handler.alloc_page().unwrap();
                handler.write_page(page1, vec![0; PAGE_SIZE - 10]).unwrap();
                let fitting_page = handler.find_fitting_page(20).unwrap();
                assert_eq!(page2.get_id(), fitting_page.unwrap().get_id());
            }

            #[test]
            fn dont_find_fitting_page_test() {
                let path = file_management::get_base_path().join("dont_find_fitting_page.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let page1 = handler.alloc_page().unwrap();
                handler.write_page(page1, vec![0; PAGE_SIZE - 10]).unwrap();
                let fitting_page = handler.find_fitting_page(90).unwrap();
                assert!(matches!(fitting_page, None), "expected none but found some");
            }

            #[test]
            fn invalid_dealloc_test() {
                let path = file_management::get_base_path().join("invalid_dealloc.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path.clone()).unwrap());
                let result = handler.dealloc_page(PageHeader::Simple(SimplePageHeader::new(999, None, 0, None, None, None)));
                assert!(result.is_err(), "Expected error when deallocating non-existent page");
            }

            #[test]
            fn free_list_integrity_test() {
                let path = file_management::get_base_path().join("free_list_integrity.test");
                file_management::delete_file(&path);
                let handler = Box::new(SimplePageHandler::new(path.clone()).unwrap());
                let page1 = handler.alloc_page().unwrap();
                let page2 = handler.alloc_page().unwrap();
                let id1 = page1.get_id();
                let id2 = page2.get_id();
                handler.dealloc_page(page1).unwrap();
                handler.dealloc_page(page2).unwrap();
                let page3 = handler.alloc_page().unwrap();
                assert_eq!(page3.get_id(), id2); // Reuse from free list
                let page4 = handler.alloc_page().unwrap();
                assert_eq!(page4.get_id(), id1); // Reuse from free list
            }

            #[test]
            fn header_conversion_test() {
                let original_header = SimplePageHeader::new(1, Some(2), 50, None, None, None);
                let header_bytes: Vec<u8> = original_header.clone().into();
                let reconstructed_header = SimplePageHeader::from( header_bytes);
                assert_eq!(original_header.id, reconstructed_header.id);
                assert_eq!(original_header.next, reconstructed_header.next);
                assert_eq!(original_header.used, reconstructed_header.used);
            }

        }

    }

}

pub mod table_management {

    use super::{file_management, page_management::{PageHandler, simple::{SimplePageHandler}}};
    use std::{io::{self, Result}, path::PathBuf};

    pub trait TableHandler {
        fn insert_row(&self, row : Vec<String>) -> io::Result<()>;
        fn delete_row(&self) -> io::Result<()>;
    }

    pub struct SimpleTableHandler {
        page_handler: Box<dyn PageHandler>
    }

    pub struct Row {
        cols : Vec<String>,
    }

    impl Row {
        fn from(cols : Vec<String>) -> Row {
            return Row { cols };
        }

        fn size(&self) -> usize {
            todo!();
        }
    }

    impl From<Vec<u8>> for Row {
        fn from(value: Vec<u8>) -> Self {
            todo!(); 
        }
    }

    impl Into<Vec<u8>> for Row {
        fn into(self) -> Vec<u8> {
            todo!(); 
        }
    }


    impl SimpleTableHandler {

        fn new(table_path : PathBuf) -> Result<SimpleTableHandler> {
            let page_handler = Box::new(SimplePageHandler::new(table_path)?);
            return Ok(SimpleTableHandler {page_handler});
        }

    }

    impl TableHandler for SimpleTableHandler {

        fn insert_row(&self, row : Vec<String>) -> io::Result<()> {
            let row = Row::from(row); 
            if let Some(id) = self.page_handler.find_fitting_page(row.size() + (usize::BITS / 8) as usize)? {

            }

            todo!(); 
        }

        fn delete_row(&self) -> io::Result<()> {
            todo!();    
        }

    }

#[cfg(test)]
    mod test {

    }

}

