use std::{fs, io::{self, Error}, path::PathBuf};
use dirs::home_dir;

pub struct FileManager {
     base_path : PathBuf
}

impl FileManager {
    pub fn new() -> FileManager {
        let mut path = home_dir().expect("Failed to find home directory");
        path.push(".d-bee");
        FileManager { base_path : path}
    }

    pub fn create_database(&self, name :&str) -> io::Result<()> {
        let mut path = self.base_path.clone(); 
        path.push("databases");
        path.push(name);
        if path.exists() {
            return Err(Error::other("database already exists"));
        }
        fs::create_dir_all(path)?;  
        return Ok(());
    }


}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_database_panic_test() {
        let fm = FileManager::new();
        fm.create_database("test").expect("Failed to create database dir");
    }

    #[test]
    fn create_database_dublicate_test() {
        let fm = FileManager::new();
        fm.create_database("double").expect("Failed to create database dir");
        fm.create_database("double").expect_err("database already exists");
    }

}
