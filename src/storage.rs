use std::{fs::{self, File}, io::{self, Error}, path::PathBuf};
use dirs::home_dir;

pub struct FileManager {
     base_path : PathBuf
}

impl FileManager {
    pub fn new() -> FileManager {
        let mut path = home_dir().expect("Unable to determine the home directory; ensure $HOME is set");
        path.push(".d-bee");
        FileManager { base_path : path}
    }

    pub fn create_database(&self, name :&str) -> io::Result<()> {
        let path = self.base_path.join("databases").join(name);
        if path.exists() {
            return Err(Error::new(io::ErrorKind::AlreadyExists, "database with this name already exists"));
        }
        fs::create_dir_all(path)?;  
        return Ok(());
    }

    pub fn create_table(&self, name : &str, database : &str) -> io::Result<()> {
        if !self.database_exists(database) {
            return Err(Error::new(io::ErrorKind::NotFound, "database does not exist"));
        }
        let path = self.base_path.join("databases").join(database).join(name);
        File::create(path)?;
        return Ok(());
    }

    fn database_exists(&self, database : &str) -> bool {
        let path = self.base_path.join("databases").join(database);
        return path.exists();
    }

    #[warn(dead_code)]
    fn table_exists(&self, name :&str, database : &str) -> bool {
        let path = self.base_path.join("databases").join(database).join(name);
        return path.exists();
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_database_normal_test() {
        let fm = FileManager::new();
        fm.create_database("test").expect("Failed to create database dir");
    }

    #[test]
    fn create_database_dublicate_test() {
        let fm = FileManager::new();
        fm.create_database("double").expect("Failed to create database dir");
        fm.create_database("double").expect_err("database with this name already exists");
    }
    
    #[test]
    fn create_table_should_work_test() {
        let fm = FileManager::new();
        fm.create_database("table_test").expect("Failed to create database dir");
        fm.create_table("test", "table_test").expect("Failed to create database");
    }
}
