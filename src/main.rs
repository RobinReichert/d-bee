mod storage;

fn main() {
    let fm = storage::FileManager::new();
    println!("{}", fm.hello_world()); 
}

