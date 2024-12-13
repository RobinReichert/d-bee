use std::{fs, io, path::PathBuf};

const BASE_PATH: &str = "~/.b-bee/";

pub struct FileManager {
     base_path : PathBuf
}

impl FileManager {
    pub fn new() -> FileManager {
        FileManager { base_path : PathBuf::from(BASE_PATH) }
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
