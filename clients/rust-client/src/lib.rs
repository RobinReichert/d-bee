use std::{net::TcpStream, io::{Result, Error, ErrorKind, Write, Read}};

const QUERY_FLAG : u8 = 0x00;
const CURSOR_FLAG : u8 = 0x01;


#[derive(Debug)]
pub enum Value {
    Text(String),
    Number(u64),
}

impl Value {

    fn new_number(bytes : Vec<u8>) -> Self {
        return Self::Number(u64::from_le_bytes(bytes.try_into().expect("expected 8 bytes")));
    }

    fn new_text(bytes : Vec<u8>) -> Self {
        return Self::Text(String::from_utf8_lossy(&bytes).to_string());
    }

}

impl TryFrom<(u64, Vec<u8>)> for Value {
    type Error = std::io::Error;

    fn try_from((type_id, bytes) : (u64, Vec<u8>)) -> std::result::Result<Self, Self::Error> {
        match type_id {
            0 => Ok(Value::new_number(bytes)),
            1 => Ok(Value::new_text(bytes)),
            _ => Err(Error::new(ErrorKind::InvalidInput, "type id did not correspond to any type")),
        }
    }
}


impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Self::Text(val) => val.clone(),
            Self::Number(val) => val.to_string(),
        }
    }
}


#[derive(Debug)]
pub struct Cursor {
    pub row : Vec<Value>,
    hash : Vec<u8>,
}

impl TryFrom<Vec<u8>> for Cursor {
    type Error = std::io::Error;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let hash : Vec<u8> = value[0..16].to_vec();
        let row : Vec<Value> = decode_row(value[16..].to_vec())?;
        return Ok(Cursor {row, hash});
    }

}

fn decode_row(bytes : Vec<u8>) -> Result<Vec<Value>> {
    let mut row : Vec<Value> = vec![];
    let mut index = 0;
    while index < bytes.len() {
        let len = u64::from_le_bytes(bytes[index..(index+8)].try_into().expect("unexpected error")) as usize; 
        index += 8;
        let type_id = u64::from_le_bytes(bytes[index..(index+8)].try_into().expect("unexpected error"));
        index += 8;
        let val = Value::try_from((type_id, bytes[index..(index+len)].try_into().expect("unexpected")))?;
        index += len;
        row.push(val);
    }
    row.reverse();
    return Ok(row);
}

pub struct Connection {
    stream : TcpStream,
}


impl Connection {

    pub fn new(address : String) -> Result<Self> {
        let stream = TcpStream::connect(&address)?;
        return Ok(Connection{stream});
    }

    pub fn query(&mut self, query : String) -> Result<Option<Cursor>> {
        let mut message : Vec<u8> = vec![];
        message.push(QUERY_FLAG);
        message.extend(query.as_bytes());
        self.stream.write_all(&message)?;
        let mut buffer = vec![0; 1024];
        let len = self.stream.read(&mut buffer)?;
        buffer.truncate(len);
        if len < 1 {
            return Err(Error::new(ErrorKind::InvalidData, "response was empty"));
        }
        match buffer.remove(0) {
            0 => Ok(Some(Cursor::try_from(buffer)?)),
            1 => Ok(None),
            2 => Err(Error::new(ErrorKind::Other, String::from_utf8_lossy(&buffer))),
            _ => Err(Error::new(ErrorKind::InvalidData, "response had invalid status code")),
        }
    }


    pub fn next(&mut self, cursor : &mut Cursor) -> Result<bool> {
        let mut message : Vec<u8> = vec![];
        message.push(CURSOR_FLAG);
        message.extend(cursor.hash.clone());
        self.stream.write_all(&message)?;
        let mut buffer = vec![0; 1024];
        let len = self.stream.read(&mut buffer)?;
        buffer.truncate(len);
        if len < 1 {
            return Err(Error::new(ErrorKind::InvalidData, "response was empty"));
        }
        match buffer.remove(0) {
            0 => {
                cursor.row = decode_row(buffer)?;
                Ok(true)
            },
            1 => Ok(false),
            2 => Err(Error::new(ErrorKind::Other, String::from_utf8_lossy(&buffer))),
            _ => Err(Error::new(ErrorKind::InvalidData, "response had invalid status code")),
        }
    }

}

#[cfg(test)]
mod tests {

    use super::*;


    #[test]
    fn o() {
        for i in 0..1000 {
            query(format!("INSERT INTO numbers VALUES ({});", i).to_string()).unwrap();
        }
    }

    #[test]
    fn t(){
        if let Some(mut res) = query("SELECT * FROM test WHERE hallo == jippy;".to_string()).unwrap() {
            println!("{:?}", res.row);
            loop {
                if !next(&mut res).unwrap() {
                    break;
                }
                println!("{:?}", res.row);
            }
        }
    }

}
