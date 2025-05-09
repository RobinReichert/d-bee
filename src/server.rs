#![allow(unused)]


use std::{io::{ErrorKind, Result, Read, Write}, thread, sync::{atomic::AtomicBool, Arc, RwLock, Mutex, Condvar}, collections::HashMap};
use mio::{Poll, Token, Interest, Events, Waker};
use mio::net::{TcpListener, TcpStream};
use rand::{Rng, thread_rng};
use crate::{executor::Executor, query::{parsing::Query}, schema::DatabaseSchemaHandler, storage::{file_management::{get_base_path, create_dir, delete_dir}, table_management::{Row, Type}}};


const QUERY_FLAG : u8 = 0x00;
const CURSOR_FLAG : u8 = 0x01;
const NEW_DATABASE_FLAG : u8 = 0x02;
const GET_KEY_FLAG : u8 = 0x03;
const TERMINATE_FLAG : u8 = 0x04;
const DELETE_DATABASE_FLAG : u8 = 0x05;


#[derive(Clone)]
pub enum ConnectionType {
    Client,
    Admin,
}



pub struct Server {
    executors : RwLock<HashMap<String, Arc<Executor>>>,
    database_schema : DatabaseSchemaHandler,
    work : Mutex<Vec<Option<Arc<Token>>>>,
    condvar : Condvar,
    connections : Mutex<HashMap<Token, (String, ConnectionType, Arc<TcpStream>)>>,
}



impl Server {

 
    pub fn new() -> Arc<Self> {

        //Set up database schema
        let path = get_base_path().expect("failed to get base path");
        let database_schema = DatabaseSchemaHandler::new(get_base_path().expect("failed to get base path")).expect("couldnt create database schema");
        let database_names = database_schema.get_database_names().expect("couldnt retrieve database names");

        //Initialize and fill executors map
        let mut executors = HashMap::new();
        for name in database_names {
            let database_path = path.join(name.clone());
            let executor = Executor::new(database_path).expect(&format!("failed to create Executor of {}", name));
            executors.insert(name, Arc::new(executor));
        }
        let work = Mutex::new(Vec::new());
        let condvar = Condvar::new();
        let connections = Mutex::new(HashMap::new());
        let mut server = Server{work, database_schema, condvar, executors: RwLock::new(executors), connections};
        let server_arc : Arc<Self> = Arc::new(server);
        return server_arc;
    }


    const SERVER : Token = Token(0);
    const ADMIN_SERVER : Token = Token(1);
    const TERMINATE : Token = Token(2);


    pub fn start(self: Arc<Self>, num_thread : usize) -> Result<()> {

        //Set up TcpListener for client and admin connections
        let mut listener :TcpListener = TcpListener::bind("127.0.0.1:4321".parse().unwrap())?;
        let mut admin_listener : TcpListener = TcpListener::bind("127.0.0.1:4322".parse().unwrap())?;

        //Map with yet unauthorized connections
        let mut pending : HashMap<Token, (ConnectionType, TcpStream)> = HashMap::new();
        let mut poll : Poll = Poll::new()?;

        //Waker is used to handle a termination event
        let waker : Arc<Waker> = Arc::new(Waker::new(poll.registry(), Self::TERMINATE)?);
        let mut events : Events = Events::with_capacity(128);
        let mut token_value = 3;
        poll.registry().register(&mut listener, Self::SERVER, Interest::READABLE)?;
        poll.registry().register(&mut admin_listener, Self::ADMIN_SERVER, Interest::READABLE)?;

        //Worker threads get set up
        let mut threads = Vec::new();
        for i in 0..num_thread {
            let server_clone : Arc<Server> = Arc::clone(&self); 
            let waker_clone = waker.clone();
            threads.push(thread::spawn(move || server_clone.handle_client(waker_clone)));
        }

        //Handle incoming events
        loop {
            poll.poll(&mut events, None)?;
            for event in events.iter() {
                match event.token() {
                    Self::TERMINATE => {

                        //Place none as poison pill into the work vec
                        if let Ok(mut work) = self.work.lock() {
                            for _ in 0..num_thread {
                                work.push(None);
                                self.condvar.notify_one();
                            }
                        }

                        //Wait for threads to finish then exit
                        for thread in threads {
                            thread.join();
                        }
                        std::process::exit(0);
                    },
                    Self::SERVER => {
                        loop {

                            //Accept incoming client connections and place them into the pending
                            //vec with the client flag
                            match listener.accept() {
                                Ok((mut stream, _)) => {
                                    let token = Token(token_value);
                                    token_value += 1;
                                    stream.set_nodelay(true);
                                    poll.registry().register(&mut stream, token, Interest::READABLE.add(Interest::WRITABLE))?;
                                    pending.insert(token, (ConnectionType::Client, stream));
                                },
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    println!("{}",e);
                                    break;
                                },
                            }
                        }
                    },
                    Self::ADMIN_SERVER => {
                        loop {

                            //Accept incoming admin connections and place them into the pending vec
                            //with an admin flag
                            match admin_listener.accept() {
                                Ok((mut stream, _)) => {
                                    let token = Token(token_value);
                                    token_value += 1;
                                    stream.set_nodelay(true);
                                    poll.registry().register(&mut stream, token, Interest::READABLE.add(Interest::WRITABLE))?;
                                    pending.insert(token, (ConnectionType::Admin, stream));
                                },
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    println!("{}",e);
                                    break;
                                },
                            }
                        }
                    }
                    token if pending.contains_key(&token) => {

                        //The first message on each connection should always be a (database and) the
                        //fitting key, otherwise the connection is ended and removed from the
                        //pending vec
                        let (connection_type, mut stream) = pending.remove(&token).unwrap();
                        let mut buff = [0u8; 512];
                        match stream.read(&mut buff) {
                            Ok(len) => {
                                if let Ok(credentials) = String::from_utf8(buff[..len].to_vec()) {
                                    match connection_type {
                                        ConnectionType::Admin => {

                                            //An admin connection does not require the database
                                            //name
                                            if self.database_schema.check_admin_key(credentials) {
                                                stream.write_all(&[0u8; 1]);
                                                stream.flush();
                                                if let Ok(mut connections) = self.connections.lock() {
                                                    let stream_arc = Arc::new(stream);
                                                    connections.insert(token, (String::new(), connection_type, stream_arc));
                                                }
                                            } else {
                                                poll.registry().deregister(&mut stream);
                                                stream.write_all(&[1u8; 1]);
                                                stream.flush();
                                            }
                                        },
                                        ConnectionType::Client => {
                                            if let Some((database, key)) = credentials.split_once(".") {
                                                match self.database_schema.check_key(database.to_string(), key.to_string()) {
                                                    Ok(true) => {
                                                        stream.write_all(&[0u8; 1]);
                                                        stream.flush();
                                                        if let Ok(mut connections) = self.connections.lock() {
                                                            let stream_arc = Arc::new(stream);
                                                            connections.insert(token, (database.to_string(), connection_type, stream_arc));
                                                        }
                                                    }
                                                    _ => {
                                                        poll.registry().deregister(&mut stream);
                                                        stream.write_all(&[1u8; 1]);
                                                        stream.flush();
                                                    },
                                                }
                                            }
                                        }
                                    }
                                }else{
                                    continue;
                                }
                            },

                            //Sometimes an event is registered but the connection is not yet ready
                            //to be read from. In this case the error is caught and the loop is
                            //continued with the connection
                            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                                pending.insert(token, (connection_type, stream));
                                break;
                            },
                            Err(e) =>{
                                println!("{}", e);
                            }
                        }
                    },
                    token => {

                        //All other incoming messages from connections are passed to the workers
                        //via the work vec
                        if let Ok(mut work) = self.work.lock() {
                            work.push(Some(Arc::new(token)));
                            self.condvar.notify_one();
                        }
                    },
                }
            }
        }
        return Ok(());
    }

    fn handle_client(self: Arc<Self>, terminate : Arc<Waker>) {
        'outer:

            //continuously wait for new work
            loop {
                let ((database, connection_type, mut stream), token) : ((String, ConnectionType, Arc<TcpStream>), Token) = match self.work.lock() {
                    Ok(mut work) => {
                        while work.is_empty() {
                            work = self.condvar.wait(work).expect("thread poisoned")
                        }
                        match work.pop().expect("unexpected error: work was empty") {
                            Some(token) => {
                                if let Ok(mut connections) = self.connections.lock() {
                                    if let Some(connection) = connections.get_mut(&token) {
                                        (connection.clone(), *token)
                                    }else {
                                        continue 'outer;
                                    }
                                }else {
                                    continue 'outer;
                                }
                            },

                            //Poison pill
                            None => return,
                        }
                    },
                    Err(_) => continue 'outer,
                };

                //Read from connection
                let mut buff = [0u8; 512];
                match stream.as_ref().read(&mut buff) {
                    Ok(0) => {
                        if let Ok(mut connections) = self.connections.lock() {
                            connections.remove(&token);
                        }else{
                            println!("error, failed to end connection");
                        }
                    }
                    Ok(len) => {
                        let mut req = buff.to_vec();
                        req.truncate(len);

                        //Check the first byte and the type of connection
                        match (connection_type, req.remove(0)) {
                            (ConnectionType::Client, QUERY_FLAG) => {
                                let q = String::from_utf8_lossy(&req).to_string();
                                self.query(database, q, stream);
                            },
                            (ConnectionType::Client, CURSOR_FLAG) => {
                                self.next(database, req.to_vec(), stream);
                            },
                            (ConnectionType::Admin, NEW_DATABASE_FLAG) => {
                                self.new_database(String::from_utf8_lossy(&req).to_string(), stream);
                            },
                            (ConnectionType::Admin, DELETE_DATABASE_FLAG) => {
                                self.delete_database(String::from_utf8_lossy(&req).to_string(), stream);
                            },
                            (ConnectionType::Admin, GET_KEY_FLAG) => {
                                self.get_key(String::from_utf8_lossy(&req).to_string(), stream);
                            },
                            (ConnectionType::Admin, TERMINATE_FLAG) => {
                                terminate.wake().expect("failed to terminate");  
                            },
                            _ => println!("Invalid flag"),
                        }
                    }

                    //If a connection was not ready to be read from ignore the work package
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        continue;
                    },
                }
            }
    }

    fn query(&self, database : String, args: String, mut stream : Arc<TcpStream>) {
        let mut response : Vec<u8> = vec![];
        match Query::from(args) {
            Ok(query) => {
                if let Ok(executors) = self.executors.read() {

                    //Choose right executor for the connection
                    if let Some(executor) = executors.get(&database) {

                        //Execute query
                        match executor.execute(query) {
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
                    } else {

                        //Couldn't read from executors
                        response.push(2);
                        response.extend("unexpected server error".as_bytes());
                    }
                }
            },
            Err(e) => {
                response.push(2);
                response.extend(e.to_string().into_bytes());
            },
        }

        //Send response
        stream.as_ref().write_all(&response);
        stream.as_ref().flush();
    }


    fn next(&self, database : String, args: Vec<u8>, mut stream : Arc<TcpStream>) {
        let mut response : Vec<u8> = vec![];
        if let Ok(executors) = self.executors.read() {
            if let Some(executor) = executors.get(&database) {

                //Args are the hash that points to the right cursor so they can be directly passed
                //to the next function
                match executor.next(args) {
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


    fn new_database(&self, args: String, mut stream : Arc<TcpStream>) {
        let mut response : Vec<u8> = vec![];
        if let Ok(base_path) = get_base_path() {

            //Args consist only of the database name
            let path = base_path.join(args.clone());

            //The directory for the executor has to be created first
            create_dir(&path); 
            match Executor::new(path) {
                Ok(executor) => {
                    let mut key = String::new();
                    let mut rng = thread_rng();
                    for i in (0..32) {
                        key.push(rng.gen_range(0x20..=0x7E).into()); 
                    }
                    if !self.database_schema.add_database(args.clone(), key.clone()).is_ok() {

                        //Send error to client and abort
                        response.push(0);
                        response.extend(b"failed to add database to schema");
                        stream.as_ref().write_all(&response);
                        stream.as_ref().flush();
                        return;
                    }

                    //If the database does not exist already the executor is inserted into the
                    //executors vec
                    if let Ok(mut executors) = self.executors.write() {
                        executors.insert(args, Arc::new(executor));
                    }
                    response.push(0);
                    response.extend(key.as_bytes());
                },
                Err(e) => {
                    response.push(0);
                    response.extend(b"failed to create executor for database: ");
                    response.extend(e.to_string().as_bytes());
                },
            }
            stream.as_ref().write_all(&response);
            stream.as_ref().flush();
        }
    }


    fn delete_database(&self, args: String, mut stream : Arc<TcpStream>) {
        let mut response : Vec<u8> = vec![];
        if let Ok(base_path) = get_base_path() {
            match self.database_schema.remove_database(args.clone()) {
                Ok(()) => {
                    let path = base_path.join(args.clone());
                    delete_dir(&path);
                    response.push(1);
                },
                Err(e) => {
                    response.push(0);
                    response.extend(b"failed to create executor for database: ");
                    response.extend(e.to_string().as_bytes());
                },
            }
            stream.as_ref().write_all(&response);
            stream.as_ref().flush();
        }
    }


    fn get_key(&self, args : String, mut stream : Arc<TcpStream>) {
        let mut response : Vec<u8> = vec![];
        match self.database_schema.get_database_key(args) {
            Ok(Some(key)) => {
                response.push(0);
                response.extend(key.as_bytes());
            },
            Ok(None) => {
                response.push(1);
                response.extend(b"database does not exist");
            },
            Err(e) => {
                response.push(1);
                response.extend(b"failed to get database key");
                response.extend(e.to_string().as_bytes());
            }
        }
        stream.as_ref().write_all(&response);
        stream.as_ref().flush();
    }


}


