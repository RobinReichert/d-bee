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

        #[derive(Debug, Clone)]
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


        #[derive(Debug, Clone)]
        pub struct Query {
            plan: HashMap<String, Argument>
        }


        use Symbol::*;


        impl Query {


            pub fn from(s: String) -> std::io::Result<Query> {
                let col_names : Symbol = Sequence(vec![t("("), r(v("column")), t(")")]);
                let col_values : Symbol = Sequence(vec![t("("), r(v("value")), t(")")]);
                let insert_values : Symbol = Option(vec![Sequence(vec![col_names.clone(), t("values"), col_values.clone()]), Sequence(vec![t("values"), col_values.clone()])]);
                let insert : Symbol = Sequence(vec![nt("insert", "command"), t("into"), v("table"), insert_values]);
                let operator : Symbol = Option(vec![nt("==", "operator"), nt("<", "operator"), nt("<=", "operator"), nt(">", "operator"), nt(">=", "operator")]);
                let predicate : Symbol = Sequence(vec![v("predicate_column_name"), operator.clone(), v("predicate_value")]);
                let columns : Symbol = Option(vec![t("*"), Sequence(vec![v("column"), r(v("column"))])]);
                let select : Symbol = Sequence(vec![nt("select", "command"), columns, t("from"), v("table"), Option(vec![Sequence(vec![t("where"), predicate.clone()]) ,Sequence(vec![]) ])]);
                let query : Symbol = Sequence(vec![Option(vec![insert, select]), t(";")]);
                let regex = Regex::new(r"\w+|[();*]|(>=)|>|(==)|<|(<=)").unwrap();
                let mut input : Vec<String> = regex.find_iter(&s).map(|x| {x.as_str()}).map(|x| {x.to_string()}).collect();
                return Ok(Query {plan: Self::solve(vec![query], input)?});
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
        fn test_valid_insert() {
            let result = Query::from("insert into test values (1 2 3);".to_string());
            assert!(result.is_ok(), "Valid insert query should not return an error");
        }

        #[test]
        fn test_valid_select() {
            let result = Query::from("select * from users where age > 30;".to_string());
            assert!(result.is_ok(), "Valid select query should not return an error");
        }

        #[test]
        fn test_missing_semicolon() {
            let result = Query::from("insert into test values (1 2 3)".to_string());
            assert!(result.is_err(), "Query without semicolon should return an error");
        }

        #[test]
        fn test_unknown_keyword() {
            let result = Query::from("update test set a = 1;".to_string());
            assert!(result.is_err(), "Unsupported 'update' query should return an error");
        }

        #[test]
        fn test_valid_insert_with_columns() {
            let result = Query::from("insert into test (col1 col2) values (1 2);".to_string());
            assert!(result.is_ok(), "Valid insert query with column names should not return an error");
        }

        #[test]
        fn test_valid_select_with_columns() {
            let result = Query::from("select col1, col2 from users where age >= 25;".to_string());
            assert!(result.is_ok(), "Valid select query with column names should not return an error");
        }

        #[test]
        fn test_valid_insert_multiple_values() {
            let result = Query::from("insert into test values (1 2 3) values (4 5 6);".to_string());
            assert!(result.is_ok(), "Valid insert query with multiple values should not return an error");
        }

        #[test]
        fn test_valid_select_without_where() {
            let result = Query::from("select col1, col2 from users;".to_string());
            assert!(result.is_ok(), "Valid select query without where clause should not return an error");
        }

        #[test]
        fn test_valid_insert_with_extra_spaces() {
            let result = Query::from("  insert   into    test   values   (  1   2   3   )  ; ".to_string());
            assert!(result.is_ok(), "Valid insert query with extra spaces should not return an error");
        }

        #[test]
        fn test_invalid_insert_missing_values() {
            let result = Query::from("insert into test (col1 col2) (1 2);".to_string());
            assert!(result.is_err(), "Insert query missing 'values' keyword should return an error");
        }

        #[test]
        fn test_invalid_insert_wrong_order() {
            let result = Query::from("insert values (1 2 3) into test;".to_string());
            assert!(result.is_err(), "Insert query with incorrect syntax should return an error");
        }

        #[test]
        fn test_invalid_select_missing_from() {
            let result = Query::from("select col1, col2 users where age > 25;".to_string());
            assert!(result.is_err(), "Select query missing 'from' keyword should return an error");
        }

        #[test]
        fn test_invalid_select_no_columns() {
            let result = Query::from("select from users;".to_string());
            assert!(result.is_err(), "Select query without column list or '*' should return an error");
        }

        #[test]
        fn test_invalid_where_condition_incomplete() {
            let result = Query::from("select * from users where age > ;".to_string());
            assert!(result.is_err(), "Select query with incomplete WHERE clause should return an error");
        }
    }


}
