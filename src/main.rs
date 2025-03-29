mod storage;
mod bubble;
mod query;
mod schema;
mod server;
mod cli;
use std::{thread, sync::{atomic::AtomicBool, Arc}};


fn main() {
    let server = server::Server::new(); 
    let terminate = Arc::new(AtomicBool::new(false));
    let terminate_clone = Arc::clone(&terminate);
    let cli_thread = thread::spawn(|| cli::start_cli(terminate_clone));
    server.start(10, terminate).expect("failed to start server");
    let _ = cli_thread.join();
}

