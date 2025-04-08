#![allow(unused)]


use std::{io::{ErrorKind, Result, Read, Write}, thread, sync::{atomic::AtomicBool, Arc, Mutex, Condvar}, collections::HashMap};
use mio::{Poll, Token, Interest, Events};
use mio::net::{TcpListener, TcpStream};
use crate::{query::{execution::Executor, parsing::Query}, storage::{file_management::get_base_path, table_management::{Row, Type}}};


const QUERY_FLAG : u8 = 0x00;
const CURSOR_FLAG : u8 = 0x01;


pub struct Server {
    executor : Arc<Executor>,
    work : Mutex<Vec<Option<Arc<TcpStream>>>>,
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

    const SERVER : Token = Token(0);

    pub fn start(self: Arc<Self>, num_thread : usize, terminate : Arc<AtomicBool>) -> Result<()> {
        let mut threads = Vec::new();
        for i in 0..num_thread {
            let server_clone : Arc<Server> = Arc::clone(&self); 
            threads.push(thread::spawn(move || server_clone.handle_client()));
        }
        let mut listener :TcpListener = TcpListener::bind("127.0.0.1:4321".parse().unwrap())?;
        let mut connections : HashMap<Token, Arc<TcpStream>> = HashMap::new();
        let mut poll : Poll = Poll::new()?;
        let mut events : Events = Events::with_capacity(128);
        let mut token_value = 1;
        poll.registry().register(&mut listener, Self::SERVER, Interest::READABLE)?;
        loop {
            poll.poll(&mut events, None)?;

            for event in events.iter() {
                match event.token() {
                    Self::SERVER => {
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
                            return Ok(());
                        }
                        match listener.accept() {
                            Ok((mut stream, _)) => {
                                let token = Token(token_value);
                                token_value += 1;
                                poll.registry().register(&mut stream, token, Interest::READABLE)?;
                                let stream_arc = Arc::new(stream);
                                connections.insert(token, stream_arc);
                            },
                            Err(e) => (),
                        }
                    },
                    token => {
                        let stream = match connections.get_mut(&token) {
                            Some(s) => s,
                            None => continue,
                        };
                        if let Ok(mut work) = self.work.lock() {
                            work.push(Some(stream.clone()));
                            self.condvar.notify_one();
                        }
                    },
                }
            }

        }
        return Ok(());
    }

    fn handle_client(self: Arc<Self>) {
        'outer:
            loop {
                let mut stream : Arc<TcpStream> = match self.work.lock() {
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
                let mut buff = [0u8; 512];
                match stream.as_ref().read(&mut buff) {
                    Ok(len) => {
                        if len < 1 {
                            continue;
                        }
                        let mut req = buff.to_vec();
                        req.truncate(len);
                        match req.remove(0) {
                            QUERY_FLAG => {
                                let q = String::from_utf8_lossy(&req).to_string();
                                self.query(q, stream);
                            },
                            CURSOR_FLAG => {
                                self.next(req.to_vec(), stream);
                            },
                            _ => println!("Invalid flag"),
                        }
                    }
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        continue;
                    },
                }
            }
    }

    fn query(&self, args: String, mut stream : Arc<TcpStream>) {
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
        stream.as_ref().write_all(&response);
        stream.as_ref().flush();
    }

    fn next(&self, args: Vec<u8>, mut stream : Arc<TcpStream>) {
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
        stream.as_ref().write_all(&response);
        stream.as_ref().flush();
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


