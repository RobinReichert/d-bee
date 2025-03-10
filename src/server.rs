#![allow(unused)]


use std::{net::{TcpListener, TcpStream}, io::{Result, Read, Write}, thread, sync::Arc};
use crate::{query::{execution::Executor, parsing::Query}, storage::{file_management::get_base_path, table_management::{Row, Type}}};


const QUERY_FLAG : u8 = 0x00;
const CURSOR_FLAG : u8 = 0x01;


pub struct Server {
    executor : Arc<Executor>,
}


impl Server {

    pub fn new() -> Self {
        let path = get_base_path().expect("failed to get base path").join("standard");
        let executor = Arc::new(Executor::new(path).expect("failed to create Executor"));
        return Server{executor};
    }

    pub fn start(self) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:4321")?;
        let server_arc : Arc<Self> = Arc::new(self);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let server_clone : Arc<Server> = Arc::clone(&server_arc); 
                    thread::spawn(move || server_clone.handle_client(stream));
                },
                Err(e) => println!("Connection failed: {}", e),
            }
        }
        return Ok(());
    }

    fn handle_client(self: Arc<Self>, mut stream: TcpStream) {
        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(length) => {
                if let Some(flag) = buffer.get(0) {
                    let data = match buffer.get(1..length) {
                        Some(slice) => slice,
                        _ => return,
                    };
                    let args = String::from_utf8_lossy(data).to_string();
                   match *flag {
                        QUERY_FLAG => {
                            self.query(args, stream);
                        },
                        CURSOR_FLAG => {
                            self.next(args, stream);
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


    fn query(&self, args: String, mut stream : TcpStream) {
        let query : Query = Query::from(args).unwrap();
        let response : Vec<u8> = match self.executor.execute(query) {
            Ok(Some(result)) => Self::encode_response(result),
            Ok(None) => b"successful".to_vec(),
            Err(e) => e.to_string().into_bytes(),
        };
        stream.write_all(&response);
        stream.flush();
    }

    fn next(&self, args: String, stream : TcpStream) {
        todo!();
    }

    fn encode_response((hash, row) : (Vec<u8>, Row)) -> Vec<u8> {
        let mut result : Vec<u8> = vec![]; 
        result.extend(hash);
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


