mod storage;

fn main() {
    let fm = storage::FileManager::new();
    let _ = fm.create_database("helloworld");
}

