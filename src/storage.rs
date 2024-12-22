#![allow(unused)]

use std::{fs::{create_dir_all, remove_dir_all, File, OpenOptions},  io::{self, ErrorKind, Read, Seek, SeekFrom, Write}, path::PathBuf};
use dirs::home_dir;

pub trait FileManager {
    fn get_base_path(&self) -> &PathBuf;

    fn create_dir(&self, path : &PathBuf) -> io::Result<()> {
        return create_dir_all(path);
    }

    fn delete_dir(&self, path : &PathBuf) -> io::Result<()> {
        return remove_dir_all(path);
    }
    
    fn create_file(&self, path : &PathBuf) -> io::Result<File> {
        File::create_new(path)
    }

    fn delete_file(&self, path : &PathBuf) -> io::Result<()> {
        remove_file(path)
    }

    fn read_at(&self, path : &PathBuf, at : u64, length : usize) -> io::Result<Vec<u8>>;

    fn write_at(&self, path : &PathBuf, at : u64, data : Vec<u8>) -> io::Result<()>;
}

pub struct SimpleFileManager {
    base_path : PathBuf,
}

impl SimpleFileManager {
    pub fn new() -> SimpleFileManager {
        let base_path = home_dir().unwrap().join(".d-bee");
        create_dir_all(&base_path);
        return SimpleFileManager {base_path};
    }

}

impl FileManager for SimpleFileManager {

    fn get_base_path(&self) -> &PathBuf {
        return &self.base_path;
    }

     fn read_at(&self, path : &PathBuf, at : u64, length : usize) -> io::Result<Vec<u8>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.seek(SeekFrom::Start(at))?;
        let mut buffer = vec![0; length];
        match file.read_exact(&mut buffer) {
            Ok(()) => Ok(buffer),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(buffer),
            Err(e) => Err(e)
        }
    }

     fn write_at(&self, path : &PathBuf, at : u64, data : Vec<u8>) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::Start(at))?;
        return file.write_all(&data);
    }

}

pub trait PageManager {
    fn read_page(&self, page_path : &PathBuf, page_id : u64) -> io::Result<Vec<u8>>;
    fn write_page(&self, page_path : &PathBuf, page_id : u64, data : Vec<u8>) -> io::Result<()>;
}

pub struct SimplePageManager<'a> {
    page_size : usize,
    file_manager : &'a dyn FileManager
}

impl<'a> SimplePageManager<'a> {
    fn new(page_size : usize, file_manager : &'a dyn FileManager) -> SimplePageManager<'a> {
        return SimplePageManager { page_size, file_manager};
    }
}

impl<'a> PageManager for SimplePageManager<'a> {
    fn read_page(&self, page_path : &PathBuf, page_id : u64) -> io::Result<Vec<u8>> {
        return self.file_manager.read_at(page_path, page_id * (self.page_size as u64), self.page_size);
    }

    fn write_page(&self, page_path : &PathBuf, page_id : u64, data : Vec<u8>) -> io::Result<()> {
        if data.len() > self.page_size {
            return Err(io::Error::new(io::ErrorKind::ArgumentListTooLong, "data is to big to write into one page"));
        }
        return self.file_manager.write_at(page_path, page_id * (self.page_size as u64), data);
    }

}

pub trait TableManager {

    fn create_table(&self, database : &str, name : &str) -> io::Result<()>;
    
    fn drop_table(&self, database : &str, name : &str) -> io::Result<()>;

    fn insert_row(&self, database : &str, table : &str) -> io::Result<()>;

    fn delete_row(&self, database : &str, table : &str) -> io::Result<()>;

}

pub struct SimpleTableManager<'a> {
    file_manager : &'a dyn FileManager,
    page_manager : &'a dyn PageManager
}

impl<'a> SimpleTableManager<'a> {

    fn new(file_manager : &'a dyn FileManager, page_manager : &'a dyn PageManager) -> SimpleTableManager<'a> {
        return SimpleTableManager {file_manager, page_manager};
    }

}

impl<'a> TableManager for SimpleTableManager<'a> {

    fn create_table(&self, database: &str, name : &str) -> io::Result<()> {
        let table_dir_path = self.file_manager.get_base_path().join("databases").join(database).join("tables");
        self.file_manager.create_dir(&table_dir_path)?;
        let table_file_path = table_dir_path.join(name);
        self.file_manager.create_file(&table_file_path)?;
        return Ok(());
    }

    fn drop_table(&self, database : &str, name : &str) -> io::Result<()> {
        let table_file_path = self.file_manager.get_base_path().join("databases").join(database).join("tables").join(name);
        return self.file_manager.delete_file(&table_file_path);
    }

fn insert_row(&self, database : &str, table : &str) -> io::Result<()> {
    todo!(); 
}

fn delete_row(&self, database : &str, table : &str) -> io::Result<()> {
    todo!();    
}

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_and_delete_test() {
        let fm : Box<dyn FileManager> = Box::new(SimpleFileManager::new());
        fm.create_file(&fm.get_base_path().join("file.rs"));
        fm.delete_file(&fm.get_base_path().join("file.rs"));
    }

    #[test]
    fn write_and_read_test() {
        let fm : Box<dyn FileManager> = Box::new(SimpleFileManager::new());
        fm.create_file(&fm.get_base_path().join("test.bee"));
        let data :Vec<u8> = "hello world".into();
        fm.write_at(&fm.get_base_path().join("test.bee"), 0, data.clone()).unwrap();
        assert_eq!(fm.read_at(&fm.get_base_path().join("test.bee"), 0, data.len()).unwrap(), data);
        fm.delete_file(&fm.get_base_path().join("test.bee"));
    }

    #[test]
    fn write_and_read_page_test() {
        let fm : &dyn FileManager = &SimpleFileManager::new();
        let pm : Box<dyn PageManager> = Box::new(SimplePageManager::new(128, fm));
        fm.create_file(&fm.get_base_path().join("page.bee"));
        let data :Vec<u8> = "hello world".into();
        pm.write_page(&fm.get_base_path().join("page.bee"), 0, data.clone()).unwrap();
        let mut res = pm.read_page(&fm.get_base_path().join("page.bee"), 0).unwrap();
        res.truncate(data.len());
        assert_eq!(res, data);
        fm.delete_file(&fm.get_base_path().join("page.bee"));

    }
}
