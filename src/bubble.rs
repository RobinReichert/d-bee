
#![allow(unused)]

use std::fmt::{self, Display, Formatter};

pub struct Bubble {
   content : String, 
   width : Vec<usize>,
}

impl Bubble {
    pub fn new(width : Vec<usize>) -> Bubble {
        let mut bubble = Bubble{width, content: String::new()};
        bubble.add_divider(); 
        return bubble;
    }

    pub fn add_divider(&mut self) {
        for j in 0..self.width.len() {
            self.content.push_str("+");
            for i in 0..self.width[j] {
                self.content.push_str("-");
            }
        }
        self.content.push_str("+");
        self.content.push_str("\n");

    }

    pub fn add_line(&mut self, content : Vec<&str>) {
        for i in 0..self.width.len() {
            self.content.push_str("|"); 
            let mut line = String::from(content[i]);
            line.truncate(self.width[i]);
            self.content.push_str(&line);
            for _ in content[i].len()..self.width[i] {
                self.content.push_str(" ");
            }
        }
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
