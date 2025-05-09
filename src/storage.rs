#![allow(unused)]

pub mod file_management {



    use std::{sync::{Mutex, Condvar}, collections::HashSet, fs::{self, create_dir_all, metadata, remove_dir_all, remove_file, File, OpenOptions}, os::unix::prelude::*, io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write}, path::PathBuf};
    use dirs::home_dir;
    use libc::{pwrite, pread};



    ///Returns working directory of this project
    pub fn get_base_path() -> Result<PathBuf> {
        return Ok(home_dir().ok_or_else(||{Error::new(ErrorKind::NotFound, "home directory could not be found")})?.join(".d-bee"));
    }



    ///Returns the directory tests files should be stored in
    #[cfg(test)]
    pub fn get_test_path() -> Result<PathBuf> {
        let path = get_base_path()?.join("test");
        if !path.is_dir() {
            create_dir_all(path.clone());
        }
        return Ok(path);
    }



    ///Create a directory with path
    pub fn create_dir(path : &PathBuf) -> Result<()> {
        return create_dir_all(path);
    }



    ///Delete the directory with path
    pub fn delete_dir(path : &PathBuf) -> Result<()> {
        return remove_dir_all(path);
    }



    ///Create a file with path
    pub fn create_file(path : &PathBuf) -> Result<File> {
        File::create_new(path)
    }



    ///Delete the file with path
    pub fn delete_file(path : &PathBuf) -> Result<()> {
        return remove_file(path);
    }



    ///Returns the size of a File
    pub fn get_size(path : &PathBuf) -> Result<u64> {
        return Ok(metadata(path)?.len());
    }



    pub trait FileHandler: Sync + Send {

        ///Returns the path this FileHandler works in
        fn get_path(&self) -> &PathBuf;

        ///Returns n bytes starting from <at>, can also return errors
        fn read_at(&self, at : usize, length : usize) -> Result<Vec<u8>>;

        ///Writes data to a file at position <at>, may return an error
        fn write_at(&self, at : usize, data : Vec<u8>) -> Result<()>;

    }



    pub struct SimpleFileHandler {

        file : File,
        fd : i32,
        path : PathBuf,
        cond : Condvar,
        accesses : Mutex<HashSet<(usize, usize)>>

    }



    impl SimpleFileHandler {


        pub fn new(path : PathBuf) -> Result<SimpleFileHandler> {
            if !path.is_file() {
                return Err(Error::new(ErrorKind::NotFound, "the path passed is not a file or does not have right permissions"));
            }
            let file = OpenOptions::new().write(true).read(true).open(&path)?;
            let fd = file.as_raw_fd();
            let cond = Condvar::new();
            let accesses = Mutex::new(HashSet::new());
            return Ok(SimpleFileHandler {file, fd, path, cond, accesses});
        }


    }



    impl FileHandler for SimpleFileHandler {


        fn get_path(&self) -> &PathBuf {
            return &self.path;
        }


        fn read_at(&self, at : usize, length : usize) -> Result<Vec<u8>> {
            {
                let mut accesses = self.accesses.lock().map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
                while accesses.iter().any(|(start, len)| *start < at + length && at < start + len){
                    accesses = self.cond.wait(accesses).map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
                }
            }
            let mut buffer = vec![0; length];
            let res = unsafe {
                pread(self.fd, buffer.as_mut_ptr() as *mut _, length, at as _)
            };
            if res == -1 {
                return Err(Error::last_os_error());
            }
            return Ok(buffer);
        }


        fn write_at(&self, at : usize, data : Vec<u8>) -> Result<()> {
            let data_len = data.len();
            {
                let mut accesses = self.accesses.lock().map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
                while accesses.iter().any(|(start, length)| *start < at + data_len && at < start + length){
                    accesses = self.cond.wait(accesses).map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
                }
                accesses.insert((at, data_len)); 
            }
            let res = unsafe {
                pwrite(self.fd, data.as_ptr() as *const _, data_len, at as _)
            };
            {
                let mut accesses = self.accesses.lock().map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
                accesses.remove(&(at, data_len)); 
                self.cond.notify_all();
            }
            if res == -1 {
                return Err(Error::last_os_error());
            }
            return Ok(());
        }


    }



#[cfg(test)]
    mod tests {



        use std::sync::{Arc};
        use std::thread;
        use super::*;



        #[test]
        //Test if directories can be created and deleted without errors
        fn create_and_delete_directory_test() {
            let dir_path = get_test_path().unwrap().join("test_dir");
            create_dir(&dir_path).unwrap();
            assert!(dir_path.is_dir(), "Directory was not created");
            delete_dir(&dir_path).unwrap();
            assert!(!dir_path.exists(), "Directory was not deleted");
        }



        #[test]
        //Test if files can be created and deleted without errors
        fn create_and_delete_file_test() {
            create_dir(&get_test_path().unwrap());
            let file_path = get_test_path().unwrap().join("create_and_delete_file.test");
            create_file(&file_path).unwrap();
            assert!(file_path.is_file(), "File was not created");
            delete_file(&file_path).unwrap();
            assert!(!file_path.exists(), "File was not deleted");
        }



        #[test]
        //Test if data can be written and read from a file without errors and if this data changes
        //during the process
        fn write_and_read_test() {
            create_dir(&get_test_path().unwrap());
            let file_path = get_test_path().unwrap().join("write_and_read.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"hello world".to_vec();
            handler.write_at(0, data.clone()).unwrap();
            let read_data = handler.read_at(0, data.len()).unwrap();
            assert_eq!(data, read_data, "Data read does not match data written");
            delete_file(&file_path).unwrap();
        }



        #[test]
        //Test if SimpleFileHandler returns an error when an invalid path is passed to the new
        //function
        fn file_not_found_test() {
            let invalid_path = get_test_path().unwrap().join("nonexistent_file.test");
            let result = SimpleFileHandler::new(invalid_path.clone());
            assert!(result.is_err(), "Expected error when initializing handler with non-existent file");
        }



        #[test]
        //Test if read_at returns a string with the right length and value
        fn read_partial_data_test() {
            create_dir(&get_test_path().unwrap());
            let file_path = get_test_path().unwrap().join("read_partial_data.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"hello world".to_vec();
            handler.write_at(0, data.clone()).unwrap();
            let read_data = handler.read_at(0, 5).unwrap(); // Read only "hello"
            assert_eq!(read_data, b"hello", "Partial read does not match expected data");
            delete_file(&file_path).unwrap();
        }



        #[test]
        //Test if data that is written and read beyond end of file is still correct
        fn write_beyond_eof_test() {
            let file_path = get_test_path().unwrap().join("write_beyond_eof.test");
            create_file(&file_path).unwrap();
            let handler: Box<dyn FileHandler> = Box::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            let data: Vec<u8> = b"beyond eof".to_vec();
            handler.write_at(100, data.clone()).unwrap();
            let read_data = handler.read_at(100, data.len()).unwrap();
            assert_eq!(read_data, data, "Data written beyond EOF does not match expected data");
            delete_file(&file_path).unwrap();
        }


        #[test]
        fn parallel_writes_test() {
            let file_path = get_test_path().unwrap().join("parallel_writes.test");
            create_file(&file_path).unwrap();
            let handler: Arc<dyn FileHandler> = Arc::new(SimpleFileHandler::new(file_path.clone()).unwrap());
            for _ in 0..1000 {
                let data1 = b"AAAA".to_vec();
                let data2 = b"BBBB".to_vec();
                let handler_clone1 = Arc::clone(&handler);
                let handler_clone2 = Arc::clone(&handler);
                let thread1 = thread::spawn(move || {
                    handler_clone1.write_at(0, data1).unwrap();
                });
                let thread2 = thread::spawn(move || {
                    handler_clone2.write_at(2, data2).unwrap();  // Overlaps partially with first write
                });
                thread1.join().unwrap();
                thread2.join().unwrap();
                let result = handler.read_at(0, 6).unwrap();
                assert!(result == b"AAAABB" || result == b"AABBBB", "Writes did not synchronize properly!");
            }
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



    pub trait PageHandler: Sync + Send {

        ///Takes the number of bytes of the data that should be stored in a page. Returns the
        ///header of the first header that can fit the data. If no page is allocated that has
        ///enough space left None is returned. May also return errors!
        fn find_fitting_page(&self, size : usize) -> Result<Option<PageHeader>>;

        ///Takes a page id and checks if the page with the id is allocated. If so the page header
        ///of that page is returned, otherwise None. May return errors!
        fn is_page(&self, id : usize) -> Result<Option<PageHeader>>;

        ///Allocate a new page and returns its page header. May return errors!
        fn alloc_page(&self) -> Result<PageHeader>;

        ///Takes a page header of the page that should be deallocated. It then gets deallocated and
        ///has to be allocated again before use. May return errors!
        fn dealloc_page(&self, page : PageHeader) -> Result<()>;

        ///Takes a page header of the page that should be read. The page bytes are then returned.
        ///May return errors!
        fn read_page(&self, page : &PageHeader) -> Result<Vec<u8>>;

        ///Takes a page header of the page that should be written to the data and the size. The
        ///size is used for the find_fitting_page method and does not necessarily have to be the
        ///length of data. May return errors!
        fn write_page(&self, page : PageHeader, data : Vec<u8>, size : usize) -> Result<()>;

        ///Takes a callback function f that gets executed for every allocated page bytes. When the
        ///callback returns true the iteration stops. Errors returned by the callback are passed
        ///through this function. Errors by this method can be returned as well!
        fn iterate_pages<'a>(&self, f : Box<dyn FnMut(PageHeader, Vec<u8>) -> Result<bool> + 'a>) -> Result<()>; 

        ///Works the same as iterate_pages but takes a page header additionally. The pages get
        ///iterated starting (inclusive) from the page corresponding to the header. May return
        ///errors!
        fn iterate_pages_from<'a>(&self, start : PageHeader, f : Box<dyn FnMut(PageHeader, Vec<u8>) -> Result<bool> + 'a>) -> Result<()>; 

    }




#[derive(Clone, Debug)]
    pub struct  PageHeader {

        ///Used to calculate the page start
        pub id : usize,

        ///Used for the find fitting_page_method
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

        
        //+------------+-------------+-------------+
        //| id         | next        | used        |
        //+------------+-------------+-------------+
        //| usize      | usize       | usize       |
        //+------------+-------------+-------------+
        //| id of      | if there    | this is     |
        //| associated | is overflow | used for    |
        //| page       | this is the | page alloc  |
        //|            | id of the   | and fitting |
        //|            | next page   | page search |
        //+------------+-------------+-------------+
        
/*
        +----+--------------------------------------------------+
        |head|next free page at: 2                              | head of free list points to the first free page 
        +----+--------------------------------------------------+
        |0   |id: 0, used: 96, next 5                           | header page contains headers of other pages 
        |0   |id: 1, used: 0, next none                         |
        |0   |id: 3, used: 0, next none                         |
        |0   |id: 4, used: 0, next none                         |
        +----+--------------------------------------------------+
        |1   |..................................................| header page of this page is 0
        +----+--------------------------------------------------+
        |2   |6                                                 | this page is not allocated anymore and page 6 is the next in free list
        +----+--------------------------------------------------+
        |3   |..................................................| header page of this page is 0
        +----+--------------------------------------------------+
        |4   |..................................................| header page of this page is 0
        +----+--------------------------------------------------+
        |5   |id: 5, used: 72, next none                        | next page of page 0 
        |5   |id: 7, used: 0, next none                         |
        |5   |id: 8, used: 0, next none                         |
        +----+--------------------------------------------------+
        |6   |9                                                 | this page is not allocated anymore and page 9 is the next free page 
        +----+--------------------------------------------------+
        |7   |..................................................| header page is 5
        +----+--------------------------------------------------+
        |8   |..................................................| header page is 5
        +----+--------------------------------------------------+
        |9   |                                                  | 
        +----+--------------------------------------------------+


*/
        impl TryFrom<Vec<u8>> for PageHeader {


            type Error = std::io::Error;


            fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
                let id = usize::from_le_bytes(value[0..8].try_into().map_err(|_| Error::new(ErrorKind::UnexpectedEof, "not enough bytes for id"))?);
                let next = usize::from_le_bytes(value[8..16].try_into().map_err(|_| Error::new(ErrorKind::UnexpectedEof, "not enough bytes for next"))?);
                let used = usize::from_le_bytes(value[16..24].try_into().map_err(|_| Error::new(ErrorKind::UnexpectedEof, "not enough bytes for used"))?);
                return Ok(PageHeader {id, used, next: if next == 0 {None} else {Some(next)}, header_page_id: None, header_offset: None, previous_page_id: None});
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



        impl PageHeader {


            fn get_first() -> PageHeader {
                return PageHeader{ header_page_id: Some(0), previous_page_id: Some(0), header_offset: Some(PageHeader::get_size()), id: 0, used: 0, next: None  }
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
                //Load previous first free page id
                let next_bytes : Vec<u8> = self.file_handler.read_at(0, 8)?;
                //Update first free page id
                self.file_handler.write_at(0, id.to_le_bytes().to_vec())?;
                //Set next free page id of the new id to the previous first
                self.file_handler.write_at(SimplePageHandler::calculate_page_start(id), next_bytes)?;
                return Ok(());
            }


            fn pop_free(&self) -> Result<usize> {
                //Load the first free page id 
                let first_page : usize = usize::from_le_bytes(self.file_handler.read_at(0, 8)?.try_into().map_err(|_|{Error::new(ErrorKind::UnexpectedEof, "not enough bytes for first page")})?);
                //Load the next free page id from the first free page
                let second_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(first_page), 8)?;
                //Check if the second free page is the tail of the free list
                if second_page_bytes != vec![0, 0, 0, 0, 0, 0, 0, 0] {
                //If it is not set the first free page to the second page
                    self.file_handler.write_at(0, second_page_bytes)?;
                }else{
                //Otherwise increment first page id by one since it has to be first free page all
                //time
                    self.file_handler.write_at(0, (first_page + 1).to_le_bytes().to_vec())?;
                }
                return Ok(first_page);
            }


            fn calculate_page_start(id : usize) -> usize {
                return id * PAGE_SIZE + HEAD_SIZE;  
            }


            ///Iterates over_all headers starting from the header passed to the function, once until true is returned from f
            fn iterate_headers_from<F>(&self, header : PageHeader, mut f : F) -> Result<()> where F : FnMut(PageHeader) -> Result<bool> {
                let mut current_page_id : usize = header.header_page_id.ok_or_else(|| {Error::new(ErrorKind::InvalidInput, "header did not contain header_page_id")})?;
                let mut previous_page_id = header.previous_page_id.ok_or_else(|| {Error::new(ErrorKind::InvalidInput, "header did not contain previous")})?;
                let mut  initial_header_offset : usize = header.header_offset.ok_or_else(||{Error::new(ErrorKind::InvalidInput, "header did not contain offset")})?;

                //Loop till the current header does not have a next_page_id
                loop {

                    //Load current header page and extract the own header in order to find the
                    //next_page_id and the number of headers stored in the page
                    let current_header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(current_page_id), PAGE_SIZE)?;
                    let own_header = PageHeader::try_from(current_header_page_bytes[0..PageHeader::get_size()].to_vec())?;

                    //Loop through all headers in the header page
                    for current_header_offset in (initial_header_offset..own_header.used).step_by(PageHeader::get_size()) {

                        //For every header set the correct header values and execute f
                        if let Some(header_bytes) = current_header_page_bytes.get(current_header_offset..current_header_offset + PageHeader::get_size()) {
                            let mut current_header = PageHeader::try_from(header_bytes.to_vec())?;
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

                    //Reset initial_offset since the offset from the header passed to the function
                    //should only be used in the first header_page
                    initial_header_offset = PageHeader::get_size();
                }
                return Ok(());
            }


        }
        


        #[cfg(test)]
        impl Display for SimplePageHandler {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let width = 50;
                let mut bubble = Bubble::new(vec![4, width]);
                let first_page : usize = usize::from_le_bytes(self.file_handler.read_at(0, 8).unwrap().try_into().unwrap());
                bubble.add_line(vec!["head".to_string(), format!("next free page at: {}", first_page.to_string())]);
                'outer:
                    for i in 0..10 {
                        let mut j : usize = 0;
                        bubble.add_divider();
                        //Check if page is a header page and if so show headers
                        loop{
                            let header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(j), PAGE_SIZE).unwrap();
                            let page_header = PageHeader::try_from(header_page_bytes[0..PageHeader::get_size()].to_vec()).unwrap();
                            if page_header.id == i {
                                for n in (0..page_header.used).step_by(PageHeader::get_size()) {
                                    let m :usize = n + PageHeader::get_size();
                                    if let Some(header_bytes) = header_page_bytes.get(n..m) {
                                        let mut header = PageHeader::try_from(header_bytes.to_vec()).unwrap();
                                        bubble.add_line(vec![i.to_string(), header.to_string()]);
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
                                bubble.add_line(vec![i.to_string(), next.to_string()]);
                                continue 'outer;
                            }
                            j = next;
                        }
                        //Write used space
                        let mut allocated = false;
                        self.iterate_headers_from(PageHeader{ header_page_id: Some(0), previous_page_id: Some(0), header_offset: Some(PageHeader::get_size()), id: 0, used: 0, next: None  },|h| {
                            if i == h.id {
                                let space = h.used * width / PAGE_SIZE;
                                let mut space_representation = String::new();
                                for _ in 0..space {
                                    space_representation.push_str("#");
                                }
                                for _ in space..width {
                                    space_representation.push_str(".");
                                }
                                bubble.add_line(vec![i.to_string(), space_representation]);
                                allocated = true;
                                return Ok(true);
                            }
                            return Ok(false);
                        });
                        if !allocated {
                            bubble.add_line(vec![i.to_string(), "".to_string()]);
                        }
                    }
                write!(f, "{}", bubble)
            }
        }



        impl PageHandler for SimplePageHandler {
            

            fn find_fitting_page(&self, size : usize) -> Result<Option<PageHeader>> {
                let mut header : Option<PageHeader> = None;
                let callback = |current_header:PageHeader| {

                    //Set header to current header and exit iteration if page fits data of size
                    if PAGE_SIZE - current_header.used >= size {
                        header = Some(current_header);
                        return Ok(true);
                    }
                    return Ok(false);
                };
                self.iterate_headers_from(PageHeader::get_first(), callback)?;
                return Ok(header);
            }


            fn is_page(&self, id : usize) -> Result<Option<PageHeader>> {
                let mut header : Option<PageHeader> = None;
                let callback = |current_header : PageHeader| {
                    if current_header.id == id {
                        header = Some(current_header);
                        return Ok(true);
                    }
                    return Ok(false);
                };
                self.iterate_headers_from(PageHeader::get_first(), callback)?;
                return Ok(header);
            }



            fn alloc_page(&self) -> Result<PageHeader> {
                let mut current_header_page_id : usize = 0;
                let mut new_page_id = self.pop_free()?;
                loop {
                    let mut current_header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(current_header_page_id), PAGE_SIZE)?;
                    let mut own_header = PageHeader::try_from(current_header_page_bytes[0..PageHeader::get_size()].to_vec())?;
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
                let header_page_id = page_header.header_page_id.ok_or_else(||{Error::new(ErrorKind::NotFound, "header did not contain header_page_id")})?;
                let mut header_page_bytes : Vec<u8> = self.file_handler.read_at(SimplePageHandler::calculate_page_start(header_page_id), PAGE_SIZE)?;
                //Remove header from header page_header
                let header_offset : usize = page_header.header_offset.ok_or(ErrorKind::InvalidInput)?;
                header_page_bytes.drain(header_offset..(header_offset + PageHeader::get_size())); 
                //Decrease used value
                let mut own_header = PageHeader::try_from(header_page_bytes[..PageHeader::get_size()].to_vec())?;
                own_header.used -= PageHeader::get_size();
                //If a header page_header is empty it gets removed
                let header_page_id = page_header.header_page_id.ok_or_else(||{Error::new(ErrorKind::NotFound, "page header did not contain a header_page_id")})?;
                if own_header.used <= PageHeader::get_size() && header_page_id != 0 {
                    let previous_page_id = page_header.previous_page_id.ok_or_else(|| {Error::new(ErrorKind::NotFound, "header did not contain previous_page_id")})?;
                    let previous_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(previous_page_id), PAGE_SIZE)?;
                    let mut previous_page_header = PageHeader::try_from(previous_page_bytes[..PageHeader::get_size()].to_vec())?;
                    previous_page_header.next = own_header.next;
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(previous_page_id), previous_page_header.into());
                }else{
                    header_page_bytes[..PageHeader::get_size()].copy_from_slice(&Into::<Vec<u8>>::into(own_header)); 
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(header_page_id), header_page_bytes)?;
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
                //Check if data fits into one page
                if data.len() > PAGE_SIZE {
                    return Err(Error::new(ErrorKind::ArgumentListTooLong, "data is to big to write into one page"));
                }
                //Load all data required to change the content of a page
                let header_page_id = page_header.header_page_id.ok_or(ErrorKind::InvalidInput)?;
                let mut header_page_bytes = self.file_handler.read_at(SimplePageHandler::calculate_page_start(header_page_id), PAGE_SIZE)?;
                let header_offset : usize = page_header.header_offset.ok_or_else(|| {Error::new(ErrorKind::NotFound, "header did not have a header_offset")})?;
                let header_bytes = header_page_bytes.get(header_offset..(header_offset + PageHeader::get_size())).ok_or_else(|| {Error::new(ErrorKind::Other, "unexpected error")})?;
                let mut own_header = PageHeader::try_from(header_bytes.to_vec())?;
                //Check if the page header passed has the same id as the header loaded from storage
                if own_header.id == page_header.id {
                    //Update size and write back header with new size as well as the page itself
                    own_header.used = size;
                    header_page_bytes[header_offset..(header_offset + PageHeader::get_size())].copy_from_slice(&Into::<Vec<u8>>::into(own_header));
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(page_header.id), data)?;
                    self.file_handler.write_at(SimplePageHandler::calculate_page_start(page_header.header_page_id.ok_or_else(||{Error::new(ErrorKind::NotFound, "page header did not contain a header_page_id")})?), header_page_bytes)?;
                    return Ok(());
                }
                //Can only be returned if header did not have the same values as the header it
                //referred to in storage
                return Err(Error::new(ErrorKind::InvalidInput, "wrong header type"));
            }


            fn iterate_pages<'a>(&self, mut f : Box<dyn FnMut(PageHeader, Vec<u8>) -> Result<bool> + 'a>) -> Result<()> {
                self.iterate_headers_from(PageHeader::get_first(),|h| {
                    return f(h.clone(), self.read_page(&h)?);
                }, )?;
                return Ok(());
            }


            fn iterate_pages_from<'a>(&self, start : PageHeader, mut f : Box<dyn FnMut(PageHeader, Vec<u8>) -> Result<bool> + 'a>) -> Result<()> {
                self.iterate_headers_from(start,|h| {
                    return f(h.clone(), self.read_page(&h)?);
                }, )?;
                return Ok(());
            }


        }



        #[cfg(test)]
        mod test {



            use super::*;



            #[test]
            fn read_write_test() {
                let path = file_management::get_test_path().unwrap().join("read_write.test");
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
                let path = file_management::get_test_path().unwrap().join("find_fitting_page.test");
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
                let path = file_management::get_test_path().unwrap().join("dont_find_fitting_page.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path).unwrap());
                let page1 = handler.alloc_page().unwrap();
                handler.write_page(page1, vec![0; PAGE_SIZE - 10], PAGE_SIZE - 10).unwrap();
                let fitting_page = handler.find_fitting_page(90).unwrap();
                assert!(matches!(fitting_page, None), "expected none but found some");
            }



            #[test]
            fn invalid_dealloc_test() {
                let path = file_management::get_test_path().unwrap().join("invalid_dealloc.test");
                file_management::delete_file(&path);
                let handler: Box<dyn PageHandler> = Box::new(SimplePageHandler::new(path.clone()).unwrap());
                let result = handler.dealloc_page(PageHeader::new(999, None, 0, None, None, None));
                assert!(result.is_err(), "Expected error when deallocating non-existent page");
            }



            #[test]
            fn free_list_integrity_test() {
                let path = file_management::get_test_path().unwrap().join("free_list_integrity.test");
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
                let reconstructed_header = PageHeader::try_from( header_bytes).unwrap();
                assert_eq!(original_header.id, reconstructed_header.id);
                assert_eq!(original_header.next, reconstructed_header.next);
                assert_eq!(original_header.used, reconstructed_header.used);
            }

        }


    }


}

pub mod table_management {



    use super::{file_management, page_management::{PageHandler, PageHeader, simple::{SimplePageHandler}}};


    use std::{
        collections::HashSet,
        io::{self, Error, ErrorKind, Result},
        path::PathBuf,
        cell::RefCell,
        fmt::{self, Display, Formatter}
    };


    use crate::bubble::Bubble;



    pub trait TableHandler: Sync + Send {

        ///Creates a row from cols and their names. They can be in the wrong order as long as val x
        ///in col_values has the same index as its corresponding name in col_names. Invalid names
        ///result in an error.
        fn cols_to_row(&self, cols_names : Option<Vec<String>>, col_values : Vec<String>) -> Result<Row>;
        
        ///Takes a row object and a col name and then Returns the value on the corresponding place
        ///in the row. If the col name is not part of the table an error is returned.
        fn get_col_from_row(&self, row : Row, col_name : &str) -> Result<Value>;

        ///Creates a Value of the type given by the table column that's name is passed to the
        ///function.
        fn create_value(&self, col_name : String, value : String) -> Result<Value>;

        ///Takes a row object and inserts it into the table this handler is working on. This
        ///method may return errors!
        fn insert_row(&self, row : Row) -> Result<()>;

        ///This method takes a predicate and returns a cursor which holds one value to a row and a
        ///reference to the next cursor which fulfill the predicates claims. In case no row does so
        ///None is returned. Errors may be returned!
        fn select_row(&self, predicate : Option<Predicate>, cols : Option<Vec<String>>) -> Result<Option<(Row, Cursor)>>;

        ///This method takes a predicate and removes all rows that fulfill the predicates claims
        ///from the table this handler works in. May fail and return an error!
        fn delete_row(&self, predicate : Option<Predicate>) -> Result<()>;

        ///Takes a cursor and updates it to point at the next row. If a next row was found this
        ///method returns true. Otherwise false is returned. Errors may be thrown!!
        fn next(&self, cursor : &mut Cursor) -> Result<Option<Row>>;

    }



#[derive(Clone, Debug, PartialEq)]
    pub enum Type {
        Text,
        Number,
    }



#[derive(Clone, Debug)]
    pub enum Value {
        Text(String),
        Number(u64),
    }



#[derive(Clone, Debug)]
    pub struct Row {
        pub cols : Vec<Value>,
    }



#[derive(Clone, Debug)]
    pub enum Operator {
        Equal,
        NotEqual,
        Less,
        LessOrEqual,
        Bigger,
        BiggerOrEqual,
    }



#[derive(Clone, Debug)]
    pub struct Predicate {
        pub column : String,
        pub operator : Operator,
        pub value : Value,
    }


#[derive(Debug)]
    pub struct Cursor {
        header : PageHeader,
        ptr_index : usize,
        data_offset : usize,
        predicate : Option<Predicate>,
        cols : Option<Vec<String>>,
    }



    impl TryFrom<u64> for Type {


        type Error = std::io::Error;


        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            Ok(match value {
                0 => Self::Number,
                1 => Self::Text,
                x => return Err(Error::new(ErrorKind::InvalidInput, format!("{} does not represent a type", x))),
            })
        }


    }



    impl TryFrom<String> for Type {


        type Error = std::io::Error;


        fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
            Ok(match value.as_str() {
                "text" => Self::Text, 
                "number" => Self::Number,
                x => return Err(Error::new(ErrorKind::InvalidInput, format!("{} does not represent a type", x))),
            })
        }


    }



    impl Into<u64> for Type {


        fn into(self) -> u64 {
            match self {
                Type::Number => 0,
                Type::Text => 1,
            }
        }


    }


    #[cfg(test)]
    impl Display for Row {


        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            let mut result  = String::new(); 
            for col in &self.cols {
                result.push_str(&col.to_string());
            }
            return write!(f, "{}", result);
        }


    }



    impl Value {


        pub fn new_text(value : String) -> Self {
            return Self::Text(value);
        }


        pub fn new_number(value : u64) -> Self {
            return Self::Number(value);
        }


        pub fn new_text_from_bytes(value : Vec<u8>) -> Result<Self> {
            return Ok(Self::Text(String::from_utf8(value).map_err(|_| Error::new(ErrorKind::InvalidInput, "couldnt convert bytes to string"))?));
        }
        

        pub fn new_number_from_bytes(value : Vec<u8>) -> Result<Self> {
            return Ok(Self::Number(u64::from_le_bytes(value.try_into().map_err(|_| Error::new(ErrorKind::InvalidInput, "couldnt convert bytes to string"))?)));
        }


    }



    impl Into<Vec<u8>> for Value {


        fn into(self) -> Vec<u8> {
            match self { 
                Self::Text(val) => {val.as_bytes().to_vec()},
                Self::Number(val) => {val.to_le_bytes().to_vec()},
            }
        }


    }

    

    impl Into<Type> for Value {


        fn into(self) -> Type {
            match self {
                Self::Text(_) => Type::Text,
                Self::Number(_) => Type::Number,
                
            }
        }


    }


    impl TryInto<String> for Value {
        type Error = std::io::Error;
        
        fn try_into(self) -> std::result::Result<String, Self::Error> {
            match self {
                Self::Text(val) => Ok(val),
                Self::Number(_) => Err(Error::new(ErrorKind::InvalidInput, "could not convert number to String")), 
            }
        }

    }


    impl TryInto<u64> for Value {
        type Error = std::io::Error;
        fn try_into(self) -> std::result::Result<u64, Self::Error> {
            match self {
                Self::Text(_) => Err(Error::new(ErrorKind::InvalidInput, "could not convert text to u64")), 
                Self::Number(val) => Ok(val),
            }
        }

    }



#[cfg(test)]
    impl Display for Value {


        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            match self { 
                Self::Text(val) => write!(f, "{}", val),
                Self::Number(val) => write!(f, "{}", val),
            }
        }


    }

    impl PartialEq for Value {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::Text(v1), Self::Text(v2)) => v1 == v2,
                (Self::Number(v1), Self::Number(v2)) => v1 == v2,
                _ => false,
            }
        }
    }



   impl TryFrom<String> for Operator {

        type Error = std::io::Error;


        fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
            Ok(match value.as_str() {
                "equal" => Self::Equal, 
                "not_equal" => Self::NotEqual,
                "less" => Self::Less,
                "less_equal" => Self::LessOrEqual,
                "bigger" => Self::Bigger,
                "bigger_equal" => Self::BiggerOrEqual,
                x => return Err(Error::new(ErrorKind::InvalidInput, format!("{} does not represent a operator", x))),
            })
        }


   }



    pub mod simple {

  
        //+------------+--------------+--------------+-----+------------------------+------------+----------------------+-----+------------+------------+
        //| row_count  | row_offset_1 | row_offset_2 | ... | row_offset_(row_count) |            | row_data_(row_count) | ... | row_data_2 | row_data_1 |
        //+------------+--------------+--------------+-----+------------------------+------------+----------------------+-----+------------+------------+
        //| OffsetType | OffsetType   | OffsetType   | ... | Offset_Type            |            | Vec<u8>              | ... | Vec<u8>    | Vec<u8>    |
        //+------------+--------------+--------------+-----+------------------------+------------+----------------------+-----+------------+------------+
        //| number of  | number of    | - || -       | ... | - || -                 | free space | contains data of one | ... | - || -     | - || -     |
        //| rows in    | bytes from   |              | ... |                        |            | row                  | ... |            |            |
        //| this page  | end of page  |              | ... |                        |            |                      | ... |            |            |
        //|            | to start of  |              | ... |                        |            |                      | ... |            |            |
        //|            | row_data     |              | ... |                        |            |                      | ... |            |            |
        //+------------+--------------+--------------+-----+------------------------+------------+----------------------+-----+------------+------------+


        use super::*;
 

        //Bytes should always be >= log_2(PAGE_SIZE)
        type OffsetType = u16;



        pub struct SimpleTableHandler {
            page_handler : Box<dyn PageHandler>,
            col_data : Vec<(Type, String)>,
        }
 

        //+--------------+--------------+-----+------------------------+------------+------------+-----+----------------------+
        //| col_offset_1 | col_offset_2 | ... | col_offset_(col_count) | col_data_1 | col_data_2 | ... | col_data_(col_count) |
        //+--------------+--------------+-----+------------------------+------------+------------+-----+----------------------+
        //| OffsetType   | OffsetType   | ... | Offset_Type            | Vec<u8>    | Vec<u8>    | ... | Vec<u8>              |
        //+--------------+--------------+-----+------------------------+------------+------------+-----+----------------------+
        //| number of    | - || -       | ... | - || -                 | contains   | - || -     | ... | - || -               |
        //| bytes from   |              | ... |                        | col_data   |            | ... |                      |
        //| start of row |              | ... |                        |            |            | ... |                      |
        //| to start of  |              | ... |                        |            |            | ... |                      |
        //| col_data     |              | ... |                        |            |            | ... |                      |
        //+--------------+--------------+-----+------------------------+------------+------------+-----+----------------------+


        impl Into<Vec<u8>> for Row {

    
            fn into(self) -> Vec<u8> {
                let mut buffer = Vec::new();
                let offset_size = (OffsetType::BITS / 8) as usize;
                buffer.resize(self.cols.len() * offset_size, 0); 
                let mut offset_cumulative : usize = self.cols.len() * offset_size;
                for (index, col) in self.cols.into_iter().enumerate() {
                    let mut col_bytes : Vec<u8> = col.into();
                    offset_cumulative += col_bytes.len();
                    buffer[index * offset_size..(index + 1) * offset_size].copy_from_slice(&OffsetType::to_le_bytes(offset_cumulative as OffsetType).to_vec());
                    buffer.append(&mut col_bytes);
                }
                return buffer;
            }


        }

        
        impl TryFrom<(Vec<u8>, Vec<Type>)> for Row {
            type Error = io::Error;

            fn try_from((bytes, col_types): (Vec<u8>, Vec<Type>)) -> std::result::Result<Self, Self::Error> {
            let offset_size = (OffsetType::BITS / 8) as usize;
            let mut last_col_offset = col_types.len() * offset_size;
            let mut row = Row {cols : Vec::new()};
            for (index, col) in col_types.iter().enumerate() {
                let col_offset = OffsetType::from_le_bytes(bytes[(index * offset_size)..((index + 1) * offset_size)].try_into().map_err(|_|{Error::new(ErrorKind::UnexpectedEof, "not enough bytes for col_offset")})?) as usize;
                let col_bytes : Vec<u8> = bytes[last_col_offset..col_offset].into();
                let val : Value = match col {
                    Type::Number => Value::new_number_from_bytes(col_bytes)?,
                    Type::Text => Value::new_text_from_bytes(col_bytes)?,
                };
                row.cols.push(val);
                last_col_offset = col_offset as usize;
            }
            return Ok(row);
        }


        }



        impl SimpleTableHandler {


           pub fn new(table_path : PathBuf, col_data: Vec<(Type, String)>) -> Result<SimpleTableHandler> {
                let page_handler = Box::new(SimplePageHandler::new(table_path)?);
                return Ok(SimpleTableHandler {page_handler, col_data});
            }


           fn row_fulfills(&self, row: &Row, p: &Option<Predicate>) -> Result<bool> {
               if let Some(predicate) = p {
                   let col_index = self.col_data.iter().position(|(t, name)| name == &predicate.column);
                   if let Some(index) = col_index {
                       if let Some(value) = row.cols.get(index) {
                           let comparison_result = match (&predicate.operator, value, &predicate.value) {
                               (Operator::Equal, Value::Text(a), Value::Text(b)) => a == b,
                               (Operator::Equal, Value::Number(a), Value::Number(b)) => a == b,
                               (Operator::NotEqual, Value::Text(a), Value::Text(b)) => a != b,
                               (Operator::NotEqual, Value::Number(a), Value::Number(b)) => a != b,
                               (Operator::Less, Value::Text(a), Value::Text(b)) => a < b,
                               (Operator::Less, Value::Number(a), Value::Number(b)) => a < b,
                               (Operator::LessOrEqual, Value::Text(a), Value::Text(b)) => a <= b,
                               (Operator::LessOrEqual, Value::Number(a), Value::Number(b)) => a <= b,
                               (Operator::Bigger, Value::Text(a), Value::Text(b)) => a > b,
                               (Operator::Bigger, Value::Number(a), Value::Number(b)) => a > b,
                               (Operator::BiggerOrEqual, Value::Text(a), Value::Text(b)) => a >= b,
                               (Operator::BiggerOrEqual, Value::Number(a), Value::Number(b)) => a >= b,
                               _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Type mismatch in comparison")),
                           };
                           return Ok(comparison_result);
                       } else {
                           return Err(io::Error::new(io::ErrorKind::InvalidInput, "Column index out of bounds"));
                       }
                   } else {
                       return Err(io::Error::new(io::ErrorKind::InvalidInput, "Column name not found in row"));
                   }
               }
               return Ok(true);
           }


           ///Checks if col names passed to the function are present in the table
           fn validate_cols(&self, col_names : Vec<String>) -> Result<()> {
               let col_name_sett: HashSet<_> = col_names.iter().collect();
               let col_data_set: HashSet<_> = self.col_data.iter().map(|(_, n)| n).collect();
               if !col_name_sett.is_subset(&col_data_set) {
                   return Err(Error::new(ErrorKind::Other, "table does not contain these cols"));
               }
               return Ok(());
           }


           ///Keeps only columns of the row that are specified in the cols vec
           fn filter_row(&self, row : &mut Row, cols : Vec<String>) -> Result<()> {
               if self.col_data.len() != row.cols.len() {
                   return Err(Error::new(ErrorKind::InvalidInput, "row was already filtered"));
               }
               self.validate_cols(cols.clone())?;
               let len = self.col_data.len();
               for i in (0..len).rev() {
                   if !cols.contains(&self.col_data[i].1) {
                       row.cols.remove(i); 
                   }
               }
               return Ok(());
           }


        }



#[cfg(test)]
        impl Display for SimpleTableHandler {


            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let (mut row, mut cursor) = self.select_row(Some(Predicate{ column: "Age".to_string(), operator: Operator::Bigger, value: Value::new_number(0)}), None).unwrap().unwrap();
                let mut bubble = Bubble::new(vec![40, 20]);
                bubble.add_line(self.col_data.iter().map(|x| x.1.clone()).collect());
                bubble.add_divider();
                loop {
                    let mut res : Vec<String> = Vec::new();
                    for col in row.cols.clone() {
                        res.push(col.to_string());
                    }
                    bubble.add_line(res);
                    if let Some(r) = self.next(&mut cursor).unwrap() {
                        row = r;
                    }else{
                        break;
                    }
                }
                return write!(f,"{}", bubble);
            }


        }




        impl TableHandler for SimpleTableHandler {


            fn get_col_from_row(&self, row : Row, col_name : &str) -> Result<Value> {
                let col_index = self.col_data.iter().position(|(t, name)| name == col_name);
                if let Some(index) = col_index {
                    if let Some(value) = row.cols.get(index) {
                        return Ok(value.clone());
                    }
                }
                return Err(Error::new(ErrorKind::InvalidInput, "col with this name was not found in row"));
            }




            fn cols_to_row(&self, mut col_names_option : Option<Vec<String>>, col_values : Vec<String>) -> Result<Row> {
                let col_names : Vec<String> = match col_names_option {
                    Some(c) => {
                        self.validate_cols(c.clone())?;
                        c
                    },
                    None => self.col_data.clone().into_iter().map(|(_, n)| n).collect(),
                };
                if col_names.len() != col_values.len() {
                    return Err(Error::new(ErrorKind::InvalidInput, "amount of values and columns did not match"));
                }
                let mut cols : Vec<(String, String)> = col_names.into_iter().zip(col_values.into_iter()).collect();
                cols.sort_by_key(|(n, _)| self.col_data.iter().position(|(_, s)| s==n));
                let mut res : Vec<Value> = vec![];
                for (index, (name, value)) in cols.iter().enumerate() {
                    let col : Result<Value> = match self.col_data[index].0 {
                        Type::Text => Ok(Value::new_text(value.clone())),
                        Type::Number => {
                            let number_value : u64 = value.parse().map_err(|_| Error::new(ErrorKind::InvalidInput, "could not convert string to int"))?;
                            Ok(Value::new_number(number_value))
                        },
                    };
                    res.push(col?);
                }
                return Ok(Row{cols: res});
            }


            fn create_value(&self, col_name : String, value : String) -> Result<Value> {
                let col = self.col_data.iter().find(|(_, n)| *n == col_name).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "col is not present in table"))?;
                Ok(match col.0 {
                    Type::Text => Value::new_text(value),
                    Type::Number => {
                        let number_value : u64 = value.parse().map_err(|_| Error::new(ErrorKind::InvalidInput, "could not convert string to int"))?;
                        Value::new_number(number_value)
                    },
                })
            }


            fn insert_row(&self, row : Row) -> Result<()> {
                let mut row_bytes : Vec<u8> = row.into();
                let row_size = row_bytes.len();
                let ptr_size = (OffsetType::BITS / 8) as usize;
                let mut used = 0;
                let page_header = match self.page_handler.find_fitting_page(row_size + ptr_size)? {
                    Some(p) => p,
                    None => {
                        used += ptr_size;
                        self.page_handler.alloc_page()?},
                };
                used += page_header.used + row_size + ptr_size;
                let mut page = self.page_handler.read_page(&page_header)?; 
                let ptr_count = OffsetType::from_le_bytes(page[0..ptr_size].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for ptr_count")})?) as usize;
                let data_offset = OffsetType::from_le_bytes(page[(ptr_count * ptr_size)..((ptr_count + 1) * ptr_size)].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for data_offset")})?) as usize;
                page[0..ptr_size].copy_from_slice(&OffsetType::to_le_bytes((ptr_count+1) as OffsetType).to_vec());
                page[((ptr_count + 1) * ptr_size)..((ptr_count + 2) * ptr_size)].copy_from_slice(&OffsetType::to_le_bytes((data_offset + row_size) as OffsetType).to_vec());
                if page.len() < data_offset + row_size {
                    return Err(Error::new(ErrorKind::InvalidInput, "page to small for input"));
                }
                let start : usize = page.len() - (data_offset + row_size);
                let end : usize = page.len() - data_offset;
                page[start..end].copy_from_slice(&row_bytes);
                self.page_handler.write_page(page_header.clone(), page, used)?;
                return Ok(());
            }



            fn delete_row(&self, predicate : Option<Predicate>) -> Result<()> {
                let col_types : Vec<Type> = self.col_data.iter().map(|x| x.0.clone()).collect();
                let callback = |header : PageHeader, mut page : Vec<u8>| -> Result<bool> {
                    let mut new_used = header.used;
                    let ptr_size = (OffsetType::BITS / 8) as usize;
                    //Get pointer count in order to then iterate over all rows in the page. 
                    let mut ptr_count = OffsetType::from_le_bytes(page[0..ptr_size].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for ptr_count")})?) as usize;
                    let mut previous_data_offset : usize = 0;
                    //Iterate over all rows in the page
                    let mut ptr_index = 0;
                    while ptr_index < ptr_count {
                        //Get offset of last page
                        let last_offset_start = (ptr_count)*ptr_size;
                        let last_offset_end = (ptr_count+1)*ptr_size;
                        let mut last_offset = OffsetType::from_le_bytes(page[last_offset_start..last_offset_end].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for last_offset")})?) as usize;
                        //Get the row
                        let current_offset_start = (ptr_index + 1) * ptr_size;
                        let current_offset_end = (ptr_index + 2) * ptr_size;
                        let data_offset = OffsetType::from_le_bytes(page[current_offset_start..current_offset_end].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for data_offset")})?) as usize;
                        let data_start : usize = page.len() - data_offset;
                        let data_end : usize = page.len() - previous_data_offset;
                        let row_bytes : Vec<u8> = page[data_start..data_end].into();
                        let value : Row = Row::try_from((row_bytes, col_types.clone()))?;
                        if self.row_fulfills(&value, &predicate)? {
                            //Shift the data left of the deleted row to the right, just over it
                            let row_size = data_end - data_start;
                            let last_data_start = page.len()-last_offset;
                            let remainder_bytes = &page[last_data_start..data_start].to_vec();
                            page[(data_end-remainder_bytes.len())..data_end].copy_from_slice(remainder_bytes);
                            for remaining_index in ptr_index..ptr_count {
                                //Shift the data_offsets to the left over the deleted data_offset
                                let start = (remaining_index + 1) * ptr_size;
                                let end = (remaining_index + 2) * ptr_size;
                                let mut new_data_offset = OffsetType::from_le_bytes(page[start..end].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for data_offset")})?) as usize;
                                new_data_offset -= row_size;
                                let new_start = remaining_index * ptr_size;
                                let new_end = (remaining_index+1) * ptr_size;
                                page[new_start..new_end].copy_from_slice(&OffsetType::to_le_bytes(new_data_offset as OffsetType).to_vec());
                            }
                            //Adjust other variables
                            new_used -= (row_size + ptr_size);
                            last_offset += row_size;
                            ptr_count -= 1;
                        }else{
                            ptr_index += 1;
                            previous_data_offset = data_offset;
                        }
                    }
                    if new_used != header.used {
                        //Write back page if it changed
                        page[0..ptr_size].copy_from_slice(&OffsetType::to_le_bytes(ptr_count as OffsetType).to_vec());
                        self.page_handler.write_page(header.clone(), page, new_used); 
                    }
                    return Ok(false);
                };
                self.page_handler.iterate_pages(Box::new(callback))?;
                return Ok(());
            }



            fn select_row(&self, predicate : Option<Predicate>, cols : Option<Vec<String>>) -> Result<Option<(Row, Cursor)>> {
                let col_types : Vec<Type> = self.col_data.iter().map(|x| x.0.clone()).collect();
                let mut result : Option<(Row, Cursor)> = None;
                let callback = |header : PageHeader, page : Vec<u8>| -> Result<bool> {
                    let ptr_size = (OffsetType::BITS / 8) as usize;
                    let ptr_count = OffsetType::from_le_bytes(page[0..ptr_size].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for ptr_count")})?) as usize;
                    let mut last_data_offset : usize = 0;
                    for ptr_index in 0..ptr_count.clone() {
                        let start = (ptr_index + 1) * ptr_size;
                        let end = (ptr_index + 2) * ptr_size;
                        let data_offset = OffsetType::from_le_bytes(page[start..end].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for data_offset")})?) as usize;
                        let start : usize = page.len() - data_offset;
                        let end : usize = page.len() - last_data_offset;
                        let row_bytes : Vec<u8> = page[start..end].into();
                        let mut row : Row = Row::try_from((row_bytes, col_types.clone()))?;
                        if self.row_fulfills(&row, &predicate)? {
                            if let Some(cs) = cols.clone() {
                                self.filter_row(&mut row, cs)?;
                            }
                            result = Some((row, Cursor { header, ptr_index: ptr_index+1, data_offset, predicate: predicate.clone(), cols: cols.clone()}));
                            return Ok(true);
                        }
                        last_data_offset = data_offset;
                    }
                    return Ok(false);
                };
                self.page_handler.iterate_pages(Box::new(callback))?;
                return Ok(result);
            }



            fn next(&self, cursor : &mut Cursor) -> Result<Option<Row>> {
                let col_types : Vec<Type> = self.col_data.iter().map(|x| x.0.clone()).collect();
                let mut result : Option<Row> = None;
                let mut found_next = false;
                let mut initial_ptr_index = cursor.ptr_index;
                let mut initial_last_data_offset = cursor.data_offset;
                self.page_handler.iterate_pages_from(cursor.header.clone(), Box::new(
                        |header : PageHeader, page : Vec<u8>| -> Result<bool> { 
                            let ptr_size = (OffsetType::BITS / 8) as usize;
                            let ptr_count = OffsetType::from_le_bytes(page[0..ptr_size].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for ptr_count")})?) as usize;
                            let mut last_data_offset : usize = initial_last_data_offset;
                            for ptr_index in initial_ptr_index..ptr_count {
                                let start = (ptr_index + 1) * ptr_size;
                                let end = (ptr_index + 2) * ptr_size;
                                let data_offset = OffsetType::from_le_bytes(page[start..end].try_into().map_err(|_| {Error::new(ErrorKind::UnexpectedEof, "not enough bytes for data_offset")})?) as usize;
                                let start : usize = page.len() - data_offset;
                                let end : usize = page.len() - last_data_offset;
                                let row_bytes : Vec<u8> = page[start..end].to_vec();
                                let mut row : Row = Row::try_from((row_bytes, col_types.clone()))?;
                                if self.row_fulfills(&row, &cursor.predicate)? {
                                    if let Some(cs) = cursor.cols.clone() {
                                        self.filter_row(&mut row, cs)?;
                                    }
                                    result = Some(row);
                                    found_next = true;
                                    cursor.header = header;
                                    cursor.data_offset = data_offset;
                                    cursor.ptr_index = ptr_index+1;
                                    return Ok(true);
                                }
                                last_data_offset = data_offset;
                            }
                            initial_ptr_index = 0;
                            initial_last_data_offset = 0;
                            return Ok(false);
                        }
                ))?;
                return Ok(result);
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
            fn type_from_string_test() {
                 assert_eq!(Type::try_from("text".to_string()).unwrap(), Type::Text);
                 assert_eq!(Type::try_from("number".to_string()).unwrap(), Type::Number);
                 Type::try_from("foo".to_string()).expect_err("foo should not be a type");

            }


            #[test]
            fn simple_table_handler_creation_test() {
                let table_path = file_management::get_test_path().unwrap().join("simple_table_handler_creation.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Number, "Age".to_string())];
                let handler_result = simple::SimpleTableHandler::new(table_path, col_data);
                assert!(handler_result.is_ok());
            }


            #[test]
            fn cols_to_row_test() {
                let table_path = file_management::get_test_path().unwrap().join("cols_to_row.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Text, "Surname".to_string()), (Type::Number, "Age".to_string())];
                let handler = simple::SimpleTableHandler::new(table_path, col_data).unwrap();

                //right order with col_names given
                let col_names : Vec<String> = vec!["Name".to_string(), "Surname".to_string(), "Age".to_string()];
                let col_values : Vec<String> = vec!["tschigerillo".to_string(), "bob".to_string(), "2".to_string()];
                let result = handler.cols_to_row(Some(col_names), col_values.clone());
                assert!(result.is_ok());
                assert_eq!(result.unwrap().cols, vec![Value::new_text("tschigerillo".to_string()), Value::new_text("bob".to_string()), Value::new_number(2)]);

                //right order without col_names
                let result = handler.cols_to_row(None, col_values.clone());
                assert!(result.is_ok());
                assert_eq!(result.unwrap().cols, vec![Value::new_text("tschigerillo".to_string()), Value::new_text("bob".to_string()), Value::new_number(2)]);

                //wrong col_names
                let col_names : Vec<String> = vec!["Wrong".to_string(), "Age".to_string(), "Name".to_string()];
                let result = handler.cols_to_row(Some(col_names), col_values.clone());
                assert!(result.is_err());

                //wrong order with col_names given
                let col_names : Vec<String> = vec!["Surname".to_string(), "Age".to_string(), "Name".to_string()];
                let col_values : Vec<String> = vec!["bob".to_string(), "2".to_string(), "tschigerillo".to_string()];
                let result = handler.cols_to_row(Some(col_names), col_values.clone());
                assert!(result.is_ok());
                assert_eq!(result.unwrap().cols, vec![Value::new_text("tschigerillo".to_string()), Value::new_text("bob".to_string()), Value::new_number(2)]);

                //wrong order without col_names
                let result = handler.cols_to_row(None, col_values);
                assert!(result.is_err());
            }


            #[test]
            fn get_col_from_row_test() {

                //create table handler
                let table_path = file_management::get_test_path().unwrap().join("get_col_from_row.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Text, "Surname".to_string()), (Type::Number, "Age".to_string())];
                let handler = simple::SimpleTableHandler::new(table_path, col_data).unwrap();

                //create row
                let col_names : Vec<String> = vec!["Name".to_string(), "Surname".to_string(), "Age".to_string()];
                let col_values : Vec<String> = vec!["tschigerillo".to_string(), "bob".to_string(), "2".to_string()];
                let row = handler.cols_to_row(Some(col_names), col_values.clone()).unwrap();
                
                //exiting col name
                let result = handler.get_col_from_row(row.clone(), "Name");
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::new_text("tschigerillo".to_string()));

                //non existent col name
                let result = handler.get_col_from_row(row, "Wrong");
                assert!(result.is_err());
            }


            #[test]
            fn create_value_test() {

                //create table handler 
                let table_path = file_management::get_test_path().unwrap().join("get_col_from_row.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Text, "Surname".to_string()), (Type::Number, "Age".to_string())];
                let handler = simple::SimpleTableHandler::new(table_path, col_data).unwrap();

                //Existing column with fitting type text
                let result = handler.create_value("Surname".to_string(), "bob".to_string());                 
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::new_text("bob".to_string()));
                
                //Existing column with fitting type number
                let result = handler.create_value("Age".to_string(), "2".to_string());
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::new_number(2));

                //Existing column with wrong type
                let result = handler.create_value("Age".to_string(), "bob".to_string());
                assert!(result.is_err());

                //Non existent column
                let result = handler.create_value("Wrong".to_string(), "bob".to_string());
                assert!(result.is_err());
            }

            #[test]
            fn row_into_bytes_and_back_test_test() {
                let row = Row {
                    cols: vec![
                        Value::new_text("text".to_string()),
                        Value::new_number(123),
                    ],
                };
                let col_types = vec![Type::Text, Type::Number];
                let row_bytes: Vec<u8> = row.clone().into();
                let reconstructed_row = simple::Row::try_from((row_bytes, col_types)).unwrap();
                assert_eq!(row.cols.len(), reconstructed_row.cols.len());
                assert_eq!(row.cols[0].to_string(), reconstructed_row.cols[0].to_string());
                assert_eq!(row.cols[1].to_string(), reconstructed_row.cols[1].to_string());
            }





            #[test]
            fn insert_and_select_test() {

                //Create table handler 
                let table_path = file_management::get_test_path().unwrap().join("insert_and_select.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Text, "Surname".to_string()), (Type::Number, "Age".to_string())];
                let handler = simple::SimpleTableHandler::new(table_path, col_data).unwrap();

                //Create rows
                let row = handler.cols_to_row(None, vec!["tschigerillo".to_string(), "bob".to_string(), "2".to_string()]).unwrap();
                let other_row = handler.cols_to_row(None, vec!["".to_string(), "alice".to_string(), "3".to_string()]).unwrap();

                //Insert the rows
                let insert_result = handler.insert_row(row.clone());
                assert!(insert_result.is_ok());
                let insert_result = handler.insert_row(other_row.clone());
                assert!(insert_result.is_ok());

                //Select and check result
                let predicate = Predicate {
                    column: "Age".to_string(),
                    operator: Operator::Equal,
                    value: Value::new_number(3),
                };
                let select_result = handler.select_row(Some(predicate), None);
                assert!(select_result.is_ok());
                let cursor_option = select_result.unwrap();
                assert!(cursor_option.is_some());
                let cursor = cursor_option.unwrap();
                assert_eq!(cursor.0.cols, other_row.cols);

                //Test with text predicate
                let other_predicate = Predicate {
                    column: "Surname".to_string(),
                    operator: Operator::Equal,
                    value: Value::new_text("bob".to_string()),
                };

                let select_result = handler.select_row(Some(other_predicate), None);
                assert!(select_result.is_ok());
                let cursor_option = select_result.unwrap();
                assert!(cursor_option.is_some());
                let cursor = cursor_option.unwrap();
                assert_eq!(cursor.0.cols, row.cols);
            }

            #[test]
            fn insert_delete_select_test() {
                let table_path = file_management::get_test_path().unwrap().join("simple_table_handler_insert_and_select.test");
                file_management::delete_file(&table_path);
                let col_data : Vec<(Type, String)> = vec![(Type::Text, "Name".to_string()), (Type::Number, "Age".to_string()), (Type::Number, "Score".to_string())];
                let handler = simple::SimpleTableHandler::new(table_path, col_data).unwrap();
                let row = Row {
                    cols: vec![
                        Value::new_text("Alice".to_string()),
                        Value::new_number(30),
                        Value::new_number(10),
                    ],
                };
                let other_row = Row{cols: vec![
                    Value::new_text("Bob".to_string()),
                    Value::new_number(10),
                    Value::new_number(5),
                ]
                };
                let third_row = Row{cols: vec![
                    Value::new_text("Chris".to_string()),
                    Value::new_number(12),
                    Value::new_number(3),
                ]
                };
                // Insert the row
                handler.insert_row(row.clone()).unwrap();
                handler.insert_row(other_row.clone()).unwrap();
                // Select the row
                let predicate = Predicate {
                    column: "Age".to_string(),
                    operator: Operator::Equal,
                    value: Value::new_number(30),
                };
                handler.delete_row(Some(predicate.clone())).unwrap();
                let select_result = handler.select_row(None, None);
                assert!(select_result.is_ok());
                let cursor_option = select_result.unwrap();
                assert!(cursor_option.is_some());
                let cursor = cursor_option.unwrap();
                assert_eq!(cursor.0.cols.len(), row.cols.len());
                assert_eq!(cursor.0.cols[0].to_string(), other_row.cols[0].to_string());
                assert_eq!(cursor.0.cols[1].to_string(), other_row.cols[1].to_string());
            }


        }



    }



#[cfg(test)]
    mod test {



        use super::*;



        #[test]
        fn test_value_display_text() {
            let text_value = Value::new_text("hello".to_string());
            assert_eq!(text_value.to_string(), "hello");
        }



        #[test]
        fn test_value_display_number() {
            let number_value = Value::new_number(42);
            assert_eq!(number_value.to_string(), "42");
        }



    }



}
