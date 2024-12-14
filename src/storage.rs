#![allow(unused)]

use std::{fs::{File, OpenOptions},  io::{self, Read, Seek, SeekFrom, Write}, path::PathBuf};
use dirs::home_dir;

pub struct FileManager {
    base_path : PathBuf,
}

impl FileManager {

    pub fn new() -> FileManager {
       let path = home_dir().unwrap().join(".d-bee");
        return FileManager {base_path: path};
    }

    pub fn create_file(&self, path : &PathBuf) -> io::Result<File> {
        File::create_new(path)
    }

    pub fn delete_file(&self, path : &PathBuf) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    pub fn read_at(&self, path : &PathBuf, at : u64, length : usize) -> io::Result<Vec<u8>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.seek(SeekFrom::Start(at))?;
        let mut buffer = vec![0; length];
        file.read_exact(&mut buffer)?;
        return Ok(buffer);
    }

    pub fn write_at(&self, path : &PathBuf, at : u64, data : Vec<u8>) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::Start(at))?;
        return file.write_all(&data);
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_and_delete_test() {
        let fm = FileManager::new();
        fm.create_file(&fm.base_path.join("file.rs"));
        fm.delete_file(&fm.base_path.join("file.rs"));
    }

    #[test]
    fn write_and_read_test() {
        let fm = FileManager::new();
        fm.create_file(&fm.base_path.join("test.txt")).unwrap();
        let data :Vec<u8> = "hello world".into();
        fm.write_at(&fm.base_path.join("test.txt"), 0, data.clone()).unwrap();
        assert_eq!(fm.read_at(&fm.base_path.join("test.txt"), 0, data.len()).unwrap(), data);
    }
}
