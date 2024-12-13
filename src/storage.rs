use std::{fs, io, path::PathBuf};
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

    pub fn create_database(&self) -> io::Result<()> {
        fs::create_dir_all(&self.base_path)?;  
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_database_panic_test() {
        let fm = FileManager::new();
        fm.create_database().expect("Failed to create database dir");
    }

}
