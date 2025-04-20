mod storage;
mod bubble;
mod query;
mod schema;
mod server;
mod cli;
use std::thread;

fn main() {

    //Server is started first so the connection by the cli_thread can be accepted.
    let server = server::Server::new(); 
    server.start(10).expect("failed to start server");
    let cli_thread = thread::spawn(|| cli::start_cli());
    let _ = cli_thread.join();
}

