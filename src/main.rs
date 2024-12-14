mod storage;

fn main() {
    let fm = storage::FileManager::new();
    let _ = fm.create_database("hello");
    let _ = fm.create_table("hello", "world");
}

