#![allow(unused)]


use std::{net::{TcpListener, TcpStream}, io::{Result, Read, Write}, thread, sync::{atomic::AtomicBool, Arc, Mutex, Condvar}};
use crate::{query::{execution::Executor, parsing::Query}, storage::{file_management::get_base_path, table_management::{Row, Type}}};


const QUERY_FLAG : u8 = 0x00;
const CURSOR_FLAG : u8 = 0x01;


pub struct Server {
    executor : Arc<Executor>,
    work : Mutex<Vec<Option<TcpStream>>>,
    condvar : Condvar,
}


impl Server {

    pub fn new() -> Arc<Self> {
        let path = get_base_path().expect("failed to get base path").join("standard");
        let executor = Arc::new(Executor::new(path).expect("failed to create Executor"));
        let work = Mutex::new(Vec::new());
        let condvar = Condvar::new();
        let terminate = Arc::new(AtomicBool::new(false));
        let mut server = Server{work, condvar, executor};
        let server_arc : Arc<Self> = Arc::new(server);
        return server_arc;
    }

    pub fn start(self: Arc<Self>, num_thread : usize, terminate : Arc<AtomicBool>) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:4321")?;
        let mut threads = Vec::new();
        for i in 0..num_thread {
            let server_clone : Arc<Server> = Arc::clone(&self); 
            threads.push(thread::spawn(move || server_clone.handle_client()));
        }
        for stream in listener.incoming() {
            if terminate.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(mut work) = self.work.lock() {
                    for _ in 0..num_thread {
                        work.push(None);
                        self.condvar.notify_one();
                    }
                }
                for thread in threads {
                    thread.join();
                }
                break;
            }
            match stream {
                Ok(stream) => {
                    if let Ok(mut work) = self.work.lock() {
                        work.push(Some(stream));
                        self.condvar.notify_one();
                    }
                },
                Err(e) => println!("Connection failed: {}", e),
            }
        }
        return Ok(());
    }

    fn handle_client(self: Arc<Self>) {
        'outer:
            loop {
                let mut stream : TcpStream = match self.work.lock() {
                    Ok(mut work) => {
                        while work.is_empty() {
                            work = self.condvar.wait(work).expect("thread poisoned")
                        }
                        match work.pop().expect("unexpected error: work was empty") {
                            Some(val) => val,
                            None => return,
                        }
                    },
                    Err(_) => continue 'outer,
                };
                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(length) => {
                        if let Some(flag) = buffer.get(0) {
                            let data = match buffer.get(1..length) {
                                Some(slice) => slice,
                                _ => return,
                            };
                            match *flag {
                                QUERY_FLAG => {
                                    self.query(String::from_utf8_lossy(data).to_string(), stream);
                                },
                                CURSOR_FLAG => {
                                    self.next(data.to_vec(), stream);
                                },
                                _ => println!("Invalid flag"),
                            }
                        }else{
                            println!("Message was empty!");
                        }
                    },
                    Err(e) => println!("Failed to read from client: {}", e),
                }
            }
    }


    fn query(&self, args: String, mut stream : TcpStream) {
        let mut response : Vec<u8> = vec![];
        match Query::from(args) {
            Ok(query) =>
                match self.executor.execute(query) {
                    Ok(Some((hash, row))) => {
                        response.push(0);
                        response.extend(hash);
                        response.extend(Self::encode_row(row));
                    },
                    Ok(None) => {
                        response.push(1);
                        response.extend(b"successful".to_vec());
                    },
                    Err(e) => {
                        response.push(2);
                        response.extend(e.to_string().into_bytes());
                    },
                }
            Err(e) => {
                response.push(2);
                response.extend(e.to_string().into_bytes());
            },
        }
        stream.write_all(&response);
        stream.flush();
    }

    fn next(&self, args: Vec<u8>, mut stream : TcpStream) {
        let mut response : Vec<u8> = vec![];
        match self.executor.next(args) {
            Ok(Some(row)) => {
                response.push(0);
                response.extend(Self::encode_row(row));
            },
            Ok(None) => {
                response.push(1);
                response.extend(b"successful".to_vec());
            },
            Err(e) => {
                response.push(2);
                response.extend(e.to_string().into_bytes());
            }
        }
        stream.write_all(&response);
        stream.flush();
    }

    fn encode_row(row : Row) -> Vec<u8> {
        let mut result : Vec<u8> = vec![]; 
        for col in row.cols {
            let col_bytes : Vec<u8> = col.clone().into();
            let col_len : u64 = col_bytes.len() as u64;
            let len_bytes : Vec<u8> = col_len.to_le_bytes().to_vec();
            let type_bytes : Vec<u8> = Into::<u64>::into(Into::<Type>::into(col)).to_le_bytes().to_vec();
            result.extend(len_bytes);
            result.extend(type_bytes);
            result.extend(col_bytes);
        }
        return result;
    }


}


