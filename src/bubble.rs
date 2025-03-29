
#![allow(unused)]

use std::fmt::{self, Display, Formatter};

pub struct Bubble {
   content : String, 
   width : Vec<usize>,
}

impl Bubble {

    pub fn get_divider(&self) -> String {
        let mut result = String::new();
        for j in 0..self.width.len() {
            result.push_str("+");
            for i in 0..self.width[j] {
                result.push_str("-");
            }
        }
        result.push_str("+");
        return result;
    }

    pub fn format_line(&self, content : Vec<String>) -> String {
        let mut result : String = String::new();
        for i in 0..self.width.len() {
            result.push_str("|"); 
            let mut line = String::from(content[i].clone());
            line.truncate(self.width[i]);
            result.push_str(&line);
            for _ in content[i].len()..self.width[i] {
                result.push_str(" ");
            }
        }
        result.push_str("|"); 
        return result;
    }

    pub fn new(width : Vec<usize>) -> Bubble {
        let mut bubble = Bubble{width, content: String::new()};
        bubble.add_divider(); 
        return bubble;
    }

    pub fn add_divider(&mut self) {
        self.content.push_str(&self.get_divider());
        self.content.push_str("\n");

    }

    pub fn add_line(&mut self, content : Vec<String>) {
        self.content.push_str(&self.format_line(content));
        self.content.push_str("|\n");
    }
}

impl Display for Bubble {
    fn fmt(&self, f : &mut Formatter) -> fmt::Result {
        let mut content = self.content.clone();
        for j in 0..self.width.len() {
            content.push_str("+");
            for i in 0..self.width[j] {
                content.push_str("-");
            }
        }
        content.push_str("+");
        content.push_str("\n");

write!(f, "{}", content)
    }
}
