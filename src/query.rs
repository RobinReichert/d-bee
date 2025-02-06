#![allow(unused)]

pub mod parser {



    use std::{io::{Result, ErrorKind, Error}, collections::hash_map::HashMap};
    use regex::Regex;



    pub struct Parser {

    }



    mod bnf {


        use super::*;


        #[derive(Clone, Debug)]
        enum Symbol {
            Terminal(String),
            NamedTerminal(String, String),
            Value(String),
            Option(Vec<Symbol>),
            Repeat(Box<Symbol>),
            Sequence(Vec<Symbol>),
        }

        #[derive(Debug)]
        enum Argument {
            Single(String),
            List(Vec<String>),
        }

        impl Argument {


            fn add(self, new: String) -> Argument {
                match self {
                    Argument::Single(x) => Argument::List(vec![x.to_string(), new]),
                    Argument::List(mut xs) => {
                        xs.push(new);
                        Argument::List(xs)
                    }
                }
            }
        }


        fn t(val: &str) -> Symbol {
            return Terminal(val.to_string());
        }

        fn nt(val: &str, name: &str) -> Symbol {
            return NamedTerminal(val.to_string(), name.to_string());
        }


        fn v(val: &str) -> Symbol {
            return Value(val.to_string());
        }

        fn r(val: Symbol) -> Symbol {
            return Repeat(Box::new(val));
        }


        pub struct Query {
        }


        use Symbol::*;


        impl Query {


            pub fn from(s: String) {
                let col_names : Symbol = Sequence(vec![t("("), r(v("column")), t(")")]);
                let col_values : Symbol = Sequence(vec![t("("), r(v("value")), t(")")]);
                let insert_values : Symbol = Option(vec![Sequence(vec![col_names.clone(), t("values"), col_values.clone()]), Sequence(vec![t("values"), col_values.clone()])]);
                let insert : Symbol = Sequence(vec![nt("insert", "command"), t("into"), v("table"), insert_values]);
                let operator : Symbol = Option(vec![nt("==", "operator"), nt("<", "operator"), nt("<=", "operator"), nt(">", "operator"), nt(">=", "operator")]);
                let predicate : Symbol = Sequence(vec![v("predicate_column_name"), operator.clone(), v("predicate_value")]);
                let columns : Symbol = Option(vec![t("*"), col_names.clone()]);
                let select : Symbol = Sequence(vec![t("select"), columns, t("from"), v("table"), Option(vec![Sequence(vec![t("where"), predicate.clone()]) ,Sequence(vec![]) ])]);
                let query : Symbol = Sequence(vec![Option(vec![insert, select]), t(";")]);
                let regex = Regex::new(r"\S+|([();])").unwrap();
                let mut input : Vec<String> = regex.find_iter(&s).map(|x| {x.as_str()}).map(|x| {x.to_string()}).collect();
                println!("input: {:?}", input);
                let res = Self::solve(vec![query], input).unwrap();
                for stack_value in res {
                    println!("{:?}", stack_value);
                }
            }


            fn solve(mut stack: Vec<Symbol>,mut input: Vec<String>) -> std::io::Result<HashMap<String, Argument>> {
                if stack.len() == 0 {
                    if input.len() > 0 {
                        return Err(Error::new(ErrorKind::InvalidInput, "input was too long"));
                    }
                    return Ok(HashMap::new()); 
                }
                match stack.pop().ok_or_else(|| {Error::new(ErrorKind::Other, "unexpected: stack was empty")})? {
                    Terminal(val) => {
                        if val == String::from(input.pop().ok_or_else(|| {
                            Error::new(ErrorKind::InvalidInput, "input was too short")
                        })?) {
                            return Self::solve(stack, input);
                        }
                        return Err(Error::new(ErrorKind::InvalidInput, "input did not conform to any syntax"));
                    },
                    NamedTerminal(val, id) => {
                        if val == String::from(input.pop().ok_or_else(|| {
                            Error::new(ErrorKind::InvalidInput, "input was too short")
                        })?) {
                            println!("{}", val);
                            let mut res = Self::solve(stack, input)?;
                            if let Some(existing) = res.insert(id.clone(), Argument::Single(val.clone())) {
                                res.remove(&val); 
                                res.insert(id, existing.add(val));
                            }
                            return Ok(res);
                        }
                        return Err(Error::new(ErrorKind::InvalidInput, "input did not conform to any syntax"));
                    }
                    Value(id) => {
                        let x = input.pop().ok_or_else(||{
                            Error::new(ErrorKind::InvalidInput, "input was too short")
                        })?;
                        let mut res = Self::solve(stack, input)?;
                        if let Some(existing) = res.insert(id.clone(), Argument::Single(x.clone())) {
                            res.remove(&id); 
                            res.insert(id, existing.add(x));
                        }
                        return Ok(res);
                    },
                    Option(options) => {
                        let mut result: std::option::Option<(HashMap<String,Argument>)> = None; 
                        for option in options {
                            let mut new_stack = stack.clone();
                            new_stack.push(option);
                            if let Ok(temp) = Self::solve(new_stack, input.clone()) {
                                result = Some(temp);
                                break;
                            }
                        }
                        return result.ok_or_else(||{
                            Error::new(ErrorKind::InvalidInput, "input did not conform to any syntax")
                        });    
                    }
                    Repeat(symbol) => {
                        if let Ok(temp) = Self::solve(stack.clone(), input.clone()) {
                            return Ok(temp);
                        } 
                        stack.push(Sequence(vec![Repeat(symbol.clone()), *symbol]));
                        Self::solve(stack, input)
                    }
                    Sequence(mut symbols) => {
                        stack.append(&mut symbols);
                        Self::solve(stack, input)
                    }
                }
            }


        }


    }



    #[cfg(test)]
    mod test {


        use super::bnf::Query;


        #[test]
        fn first_test(){
            Query::from("insert into test values ( 1 2 2 ) ;".to_string());
        }


    }



}
