pub struct FileManager {

}

impl FileManager {
    pub fn new() -> FileManager {
        FileManager {  }
    }
    pub fn hello_world(&self) -> String {
        "hello world".to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn hello_world_test() {
        let fm = FileManager::new();
        assert_eq!(fm.hello_world(), "hello worl");
    }
}
