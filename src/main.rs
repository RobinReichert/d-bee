mod storage;
mod bubble;

use crate::storage::{page_management::{PageHandler, SimplePageHandler}, file_management::get_base_path};

fn main() {
    let handler : Box<dyn PageHandler> = Box::new(SimplePageHandler::new(get_base_path().join("real_test.bee")).unwrap());
    let page = handler.alloc_page().unwrap();
    //let _ = handler.dealloc_page(page);
}

