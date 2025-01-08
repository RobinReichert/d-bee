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
        fn create_and_delete_test() {
            create_file(&get_base_path().join("create_and_delete.test")).unwrap();
            delete_file(&get_base_path().join("create_and_delete.test")).unwrap(); 
        }

        #[test]
        fn write_and_read_test() {
            create_file(&get_base_path().join("write_and_read.test"));
            let fh : Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(get_base_path().join("write_and_read.test")).unwrap());
            let data :Vec<u8> = "hello world".into();
            fh.write_at(0, data.clone()).unwrap();
            assert_eq!(fh.read_at(0, data.len()).unwrap(), data);
            delete_file(&get_base_path().join("write_and_read.test"));
        }

    }

}

pub mod page_management {

    use std::{ io::{Error, ErrorKind, Result}, path::PathBuf, usize, fmt::{self, Display, Formatter}};
    use super::file_management::{self, create_file, FileHandler, SimpleFileHandler};
    use crate::bubble::Bubble;

    const PAGE_SIZE : usize = 128;
    const HEAD_SIZE : usize = 8;

    pub trait PageHandler : Display {
        fn find_fitting_page(&self, size : usize) -> Result<Option<usize>>;
        fn alloc_page(&self) -> Result<usize>;
        fn dealloc_page(&self, id : usize) -> Result<()>;
        fn read_page(&self, id : usize) -> Result<Vec<u8>>;
        fn write_page(&self, id : usize, data : Vec<u8>) -> Result<()>;
    }

    pub struct SimplePageHandler {
        file_handler : Box<dyn FileHandler>
    }

    pub struct PageHeader {
        id : usize,
        next : Option<usize>,
        used : usize,
    }

    impl PageHeader {
        fn new(id : usize, next : Option<usize>, used : usize) -> PageHeader {
            return PageHeader{id, next, used};
        }

        fn get_size() -> usize {
            return 24;
        }
    }

    impl From<Vec<u8>> for PageHeader {
        fn from(value: Vec<u8>) -> Self {
            let id = usize::from_le_bytes(value[0..8].try_into().unwrap());
            let next = usize::from_le_bytes(value[8..16].try_into().unwrap());
            let used = usize::from_le_bytes(value[16..24].try_into().unwrap());
            return PageHeader { id, next: if next == 0 {None} else {Some(next)}, used };
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

    impl ToString for PageHeader {
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
                let first_header = PageHeader::new(0, None, PageHeader::get_size());
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

        fn iterate_headers<F>(&self, mut f : F) -> Result<()> where F : FnMut(PageHeader) -> bool {
            let mut i : usize = 0;
            loop {
                let header_page_bytes = self.read_page(i)?;
                let mut  n : usize = 1;
                loop {
                    let min : usize = n * PageHeader::get_size();
                    let max :usize = min + PageHeader::get_size();
                    if let Some(header_bytes) = header_page_bytes.get(min..max) {
                        let mut header : PageHeader = header_bytes.to_vec().into();
                        if f(header) {
                            return Ok(());
                        }
                    }else{
                        break;
                    }
                    n += 1;
                }
                let header : PageHeader = header_page_bytes[0..PageHeader::get_size()].to_vec().into();
                if let Some(next) = header.next {
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
            for i in 0..25 {
                let mut j : usize = 0;
                bubble.add_divider();
                //Check if page is a header page and if so show headers
                loop{
                    let header_page_bytes = self.read_page(j).unwrap();
                    let page_header : PageHeader = header_page_bytes[0..PageHeader::get_size()].to_vec().into();
                    if page_header.id == i {
                        for n in (0..page_header.used).step_by(PageHeader::get_size()) {
                            let m :usize = n + PageHeader::get_size();
                            if let Some(header_bytes) = header_page_bytes.get(n..m) {
                                let mut header : PageHeader = header_bytes.to_vec().into();
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
                    println!("{}", next);
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
        fn find_fitting_page(&self, size : usize) -> Result<Option<usize>> {
            let mut id : Option<usize> = None;
            self.iterate_headers(|h| {
                if PAGE_SIZE - h.used > size {
                    id = Some(h.id);
                    return true;
                }
                return false;
            })?;
            return Ok(id);
        }

        fn alloc_page(&self) -> Result<usize> {
            let mut i : usize = 0;
            loop {
                let new_page_id = self.pop_free()?;
                let mut page_bytes = self.read_page(i)?;
                let mut own_header : PageHeader = page_bytes[0..PageHeader::get_size()].to_vec().into();
                if PAGE_SIZE - own_header.used > PageHeader::get_size() {
                    //Add new header to the header page
                    let new_header_bytes : Vec<u8> = PageHeader::new(new_page_id, None, 0).into();
                    page_bytes[own_header.used..own_header.used+PageHeader::get_size()].copy_from_slice(&new_header_bytes);
                    //Increase used value
                    own_header.used += PageHeader::get_size();
                    page_bytes[..PageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                    self.write_page(i, page_bytes)?;
                    return Ok(new_page_id);
                }
                if let Some(next) = own_header.next {
                    //In case one header page did not have enough space for another header and
                    //another one exists already the loop gets repeated with the next header page
                    i = next;     
                }else{
                    //In case one page is full and no next was created a new one is appended to the
                    //previous page.
                    own_header.next = Some(new_page_id);
                    let header_bytes : Vec<u8> = own_header.into();
                    page_bytes[..PageHeader::get_size()].copy_from_slice(&header_bytes); 
                    self.write_page(i, page_bytes);
                    let new_own_header = PageHeader::new(new_page_id, None, PageHeader::get_size());
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(new_page_id), new_own_header.into());
                    i = new_page_id;
                }
            }
            return Err(Error::new(ErrorKind::Other, "unexpected error"));
        }

        fn dealloc_page(&self, id : usize) -> Result<()> {
            let mut i : usize = 0;
            let mut prev = i;
            loop {
                let mut page_bytes = self.read_page(i)?;
                let mut  n : usize = 1;
                loop {
                    let min : usize = n * PageHeader::get_size();
                    let max :usize = min + PageHeader::get_size();
                    if let Some(header_bytes) = page_bytes.get(min..max) {
                        let mut header : PageHeader = header_bytes.to_vec().into();
                        if header.id == id {
                            if let Some(next) = header.next {
                                self.dealloc_page(next);
                            }
                            //Remove header from header page
                            page_bytes.drain(min..max); 
                            //Decrease used value
                            let mut own_header : PageHeader = page_bytes[..PageHeader::get_size()].to_vec().into();
                            own_header.used -= PageHeader::get_size();
                            //If a header page is empty it gets removed
                            if own_header.used <= PageHeader::get_size() && i != 0 {
                                let mut prev_header : PageHeader = self.read_page(prev)?[..PageHeader::get_size()].to_vec().into();    
                                prev_header.next = own_header.next;
                                self.write_page(prev, Into::<Vec<u8>>::into(prev_header));
                            }else{
                                page_bytes[..PageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                                self.write_page(i, page_bytes)?;
                            }
                            //Add page to free list
                            self.push_free(id);
                            return Ok(());
                        }
                    }else{
                        break;
                    }
                    n += 1;
                }
                let own_header : PageHeader = page_bytes[0..PageHeader::get_size()].to_vec().into();
                if let Some(next) = own_header.next {
                    prev = i;
                    i = next;
                }else{
                    break;
                }
            }
            return Err(Error::new(ErrorKind::InvalidInput, "page is not allocated"));
        }

        fn read_page(&self, id : usize) -> Result<Vec<u8>> {
            return self.file_handler.read_at(SimplePageHandler::calculate_page_start(id), PAGE_SIZE);
        }

        fn write_page(&self, id : usize, data : Vec<u8>) -> Result<()> {
            if data.len() > PAGE_SIZE {
                return Err(Error::new(ErrorKind::ArgumentListTooLong, "data is to big to write into one page"));
            }
            return self.file_handler.write_at(SimplePageHandler::calculate_page_start(id), data);
        }
    }

#[cfg(test)]
    mod test {
        use crate::storage::file_management;

        use super::{SimplePageHandler, PageHandler};

        #[test]
        fn alloc_dealloc_test() {
            let path = file_management::get_base_path().join("alloc_dealloc.test");
            file_management::delete_file(&path);
            let handler : Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path.clone()).unwrap());
            assert_eq!(1, handler.alloc_page().unwrap());
            assert_eq!(2, handler.alloc_page().unwrap());
            assert_eq!(3, handler.alloc_page().unwrap());
            assert_eq!(4, handler.alloc_page().unwrap());
            handler.dealloc_page(2).unwrap();
            handler.dealloc_page(3).unwrap();
            handler.dealloc_page(1).unwrap();
            assert_eq!(1, handler.alloc_page().unwrap());
            assert_eq!(3, handler.alloc_page().unwrap());
            assert_eq!(2, handler.alloc_page().unwrap());
            assert_eq!(6, handler.alloc_page().unwrap());
        }
    }
}

pub mod table_management {

    use super::{file_management, page_management::{PageHandler, SimplePageHandler}};
    use std::{io::{self, Result}, path::PathBuf};

    pub trait TableHandler {
        fn insert_row(&self) -> io::Result<()>;
        fn delete_row(&self) -> io::Result<()>;
    }

    pub struct SimpleTableHandler {
        page_handler: Box<dyn PageHandler>
    }

    impl SimpleTableHandler {

        fn new(table_path : PathBuf) -> Result<SimpleTableHandler> {
            let page_handler = Box::new(SimplePageHandler::new(table_path)?);
            return Ok(SimpleTableHandler {page_handler});
        }

    }

    impl TableHandler for SimpleTableHandler {

        fn insert_row(&self) -> io::Result<()> {
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

