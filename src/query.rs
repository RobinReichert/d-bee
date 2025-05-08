#![allow(unused)]

pub mod parsing {



    use crate::storage::table_management::Type;
    use std::{io::{Result, ErrorKind, Error}, collections::hash_map::HashMap};
    use regex::Regex;



    mod bnf {



        use super::*;



        #[derive(Clone, Debug)]
        pub enum Symbol {

            ///A terminal represents a word and is always at the lowest level
            Terminal(String),

            ///A wrapper wraps around another Symbol and adds a key value pair to the result map
            ///when solved
            Wrapper(Box<Symbol>, String, String),

            ///A value is like a wildcard and adds a key value pair to the result map
            Value(String),

            ///An option will accept if any of the given symbols is found
            Option(Vec<Symbol>),

            ///Repeat will accept if the symbol is present 0 to n times
            Repeat(Box<Symbol>),

            ///A Sequence contains symbols and accepts if the symbols are present in the given
            ///order
            Sequence(Vec<Symbol>),
        }



        ///Terminal
        pub fn t(val: &str) -> Symbol {
            return Terminal(val.to_string());
        }



        ///Wrapper
        pub fn w(s: Symbol, key: &str, val: &str) -> Symbol {
            return Wrapper(Box::new(s), key.to_string(), val.to_string());
        }



        ///Value
        pub fn v(val: &str) -> Symbol {
            return Value(val.to_string());
        }



        ///Option
        pub fn o(os: Vec<Symbol>) ->Symbol {
            return Option(os);
        }



        ///Repeat
        pub fn r(val: Symbol) -> Symbol {
            return Repeat(Box::new(val));
        }



        ///Sequence
        pub fn s(mut ss: Vec<Symbol>) ->Symbol {
            ss.reverse();
            return Sequence(ss);
        }



        use Symbol::*;




        ///Recursively checks if the input matches the Symbol tree passed to stack and creates a
        ///map containing values defined by the Symbol tree
        pub fn solve(mut stack: Vec<Symbol>,mut input: Vec<String>) -> std::result::Result<HashMap<String, Vec<String>>, (std::io::Error, usize)> {

            //Abort
            if stack.len() == 0 {
                if input.len() > 0 {
                    return Err((Error::new(ErrorKind::InvalidInput, "input was too long"), input.len()));
                }
                return Ok(HashMap::new()); 
            }

            //Take the first Symbol of the Stack
            match stack.pop().ok_or_else(|| {(Error::new(ErrorKind::Other, "unexpected: stack was empty"), input.len())})? {
                Terminal(exp) => {

                    //Continue without the first word of the input
                    let val = String::from(input.pop().ok_or_else(|| {
                        (Error::new(ErrorKind::InvalidInput, "input was too short"), input.len())
                    })?);
                    if exp == val {
                        return solve(stack, input);
                    }
                    return Err((Error::new(ErrorKind::InvalidInput, format!("did not extpect {}, you may want to use {}", val, exp)), input.len()));
                },
                Wrapper(symbol, key, val) => {

                    //Add contained symbol to the stack and adds key value pair to the result map
                    stack.push(*symbol);
                    let mut res = solve(stack, input)?;
                    if let Some(mut existing) = res.insert(key.clone(), vec![val.clone()]) {
                        res.remove(&key); 
                        existing.push(val);
                        res.insert(key, existing);
                    }
                    return Ok(res);
                }
                Value(id) => {

                    //Removes first word of input and adds it to the result map with the key
                    //defined by the Symbol
                    let val = input.pop().ok_or_else(||{
                        (Error::new(ErrorKind::InvalidInput, "input was too short"), input.len())
                    })?;
                    let mut res = solve(stack, input)?;
                    if let Some(mut existing) = res.insert(id.clone(), vec![val.clone()]) {
                        res.remove(&id); 
                        existing.push(val);
                        res.insert(id, existing);
                    }
                    return Ok(res);
                },
                Option(options) => {

                    //Try each of the possible options and continue with the first that works
                    let mut result: std::result::Result<HashMap<String,Vec<String>>, (Error, usize)> = Err((Error::new(ErrorKind::InvalidInput, "option had no value"), input.len()));
                    let mut current_depth = usize::max_value();
                    for option in options {
                        let mut new_stack = stack.clone();
                        new_stack.push(option);
                        let temp = solve(new_stack, input.clone());
                        if temp.is_ok() {
                            result = temp;
                            break;
                        } else if let Err((_, depth)) = temp {
                            if depth < current_depth {
                                current_depth = depth;
                                result = temp;
                            }
                        }
                    }
                    return result;
                }
                Repeat(symbol) => {

                    //Try if input can be solved with current length
                    if let Ok(temp) = solve(stack.clone(), input.clone()) {
                        return Ok(temp);
                    } 

                    //If it failed continue with one more iteration
                    stack.push(Sequence(vec![Repeat(symbol.clone()), *symbol]));
                    solve(stack, input)
                }
                Sequence(mut symbols) => {

                    //Add all contained symbols to stack and continue
                    stack.append(&mut symbols);
                    solve(stack, input)
                }
            }
        }



    }


    pub const COMMAND_KEY : &str = "command";
    pub const CREATE : &str = "create";
    pub const DROP : &str = "drop";
    pub const INSERT : &str = "insert";
    pub const SELECT : &str = "select";
    pub const DELETE : &str = "delete";
    pub const TABLE_NAME_KEY : &str = "table_name";
    pub const COLUMN_NAME_KEY : &str = "column_name";
    pub const COLUMN_TYPE_KEY : &str = "column_type";
    pub const COLUMN_VALUE_KEY : &str = "column_value";
    pub const NUMBER : &str = "number";
    pub const TEXT : &str = "text";
    pub const OPERATOR_KEY : &str = "operator";
    pub const EQUAL : &str = "equal";
    pub const NOT_EQUAL : &str = "not_equal";
    pub const SMALLER : &str = "less";
    pub const SMALLER_EQUAL : &str = "less_equal";
    pub const BIGGER : &str = "bigger";
    pub const BIGGER_EQUAL : &str = "bigger_equal";
    pub const PREDICATE_COL : &str = "predicate_col";
    pub const PREDICATE_VAL : &str = "predicate_val";




    use bnf::*;



    #[derive(Debug, Clone)]
    pub struct Query {
        pub plan: HashMap<String, Vec<String>>
    }



    impl Query {


        pub fn from(q: String) -> std::io::Result<Query> {

            //Definition of all possible SQL commands
            let data_type : Symbol = o(vec![w(t("text"), COLUMN_TYPE_KEY, TEXT), w(t("number"), COLUMN_TYPE_KEY, NUMBER)]);

            let col_data : Symbol = o(vec![
                s(vec![v(COLUMN_NAME_KEY), data_type.clone()]), 
                s(vec![r(
                        s(vec![v(COLUMN_NAME_KEY), data_type.clone(), t(",")])),
                        s(vec![v(COLUMN_NAME_KEY), data_type])])]);

            let create_table : Symbol = w(s(vec![t("create"), t("table"), v(TABLE_NAME_KEY), t("("), col_data, t(")")]), COMMAND_KEY, CREATE);

            let drop_table : Symbol = w(s(vec![t("drop"), t("table"), v(TABLE_NAME_KEY)]), COMMAND_KEY, DROP);

            let col_names : Symbol = o(vec![s(vec![]), v(COLUMN_NAME_KEY), s(vec![r(s(vec![v(COLUMN_NAME_KEY), t(",")])), v(COLUMN_NAME_KEY)])]);

            let col_values : Symbol = o(vec![s(vec![]), v(COLUMN_VALUE_KEY), s(vec![r(s(vec![v(COLUMN_VALUE_KEY), t(",")])), v(COLUMN_VALUE_KEY)])]);

            let insert_values : Symbol = o(vec![s(vec![t("("), col_names.clone(), t(")"), t("values"), t("("), col_values.clone(), t(")")]), s(vec![t("values"), t("("), col_values.clone(), t(")")])]);

            let insert : Symbol = w(s(vec![t("insert"), t("into"), v(TABLE_NAME_KEY), insert_values]), COMMAND_KEY, INSERT);

            let operator : Symbol = o(vec![
                w(t("=="), OPERATOR_KEY, EQUAL), 
                w(t("!="), OPERATOR_KEY, NOT_EQUAL), 
                w(t("<"), OPERATOR_KEY, SMALLER), 
                w(t("<="), OPERATOR_KEY, SMALLER_EQUAL), 
                w(t(">"), OPERATOR_KEY, BIGGER), 
                w(t(">="), OPERATOR_KEY, BIGGER_EQUAL)]);

            let predicate : Symbol = o(vec![s(vec![]), s(vec![t("where"), v(PREDICATE_COL), operator.clone(), v(PREDICATE_VAL)])]);

            let columns : Symbol = o(vec![t("*"), v(COLUMN_NAME_KEY), s(vec![r(s(vec![v(COLUMN_NAME_KEY), t(",")])), v(COLUMN_NAME_KEY)])]);

            let select : Symbol = w(s(vec![t("select"), columns, t("from"), v(TABLE_NAME_KEY), predicate.clone()]), COMMAND_KEY, SELECT);

            let delete : Symbol = w(s(vec![t("delete"), t("from"), v(TABLE_NAME_KEY), predicate.clone()]), COMMAND_KEY, DELETE);

            let query : Symbol = s(vec![o(vec![create_table, drop_table, insert, select, delete]), t(";")]);

            //Split query string to create input for bnf solver
            let regex = Regex::new(r"\w+|[();,*]|>=|>|==|!=|<|<=").unwrap();
            let mut input : Vec<String> = regex.find_iter(&q.to_lowercase()).map(|x| {x.as_str()}).map(|x| {x.to_string()}).collect();
            input.reverse();

            //Solve
            let plan = bnf::solve(vec![query], input).map_err(|e|{Error::new(ErrorKind::InvalidInput, e.0.to_string())});
            return Ok(Query {plan: plan?});
        }




    }



    #[cfg(test)]
    mod test {


        use super::*;


        #[test]
        fn test_valid_create_table() {
            let result = Query::from("CREATE TABLE test (hallo TEXT);".to_string());
            assert!(result.is_ok(), "Valid create query should not return an error");
        }


        #[test]
        fn test_valid_insert_with_columns() {
            let result = Query::from("INSERT INTO test (col1, col2) VALUES (1, 2);".to_string());
            assert!(result.is_ok(), "Valid insert query with column names should not return an error");
        }


        #[test]
        fn test_valid_select_with_columns() {
            let result = Query::from("SELECT col1, col2 FROM users WHERE age >= 25;".to_string());
            assert!(result.is_ok(), "Valid select query with column names should not return an error");
        }


        #[test]
        fn test_invalid_insert_multiple_values() {
            let result = Query::from("INSERT INTO test VALUES (1, 2, 3) VALUES (4, 5, 6);".to_string());
            assert!(result.is_err(), "Valid insert query with multiple values should return an error");
        }


        #[test]
        fn test_valid_select_without_where() {
            let result = Query::from("SELECT col1, col2 FROM users;".to_string());
            assert!(result.is_ok(), "Valid select query without WHERE clause should not return an error");
        }


        #[test]
        fn test_valid_insert_with_extra_spaces() {
            let result = Query::from("   INSERT   INTO    test   VALUES   (  1,   2,   3   )  ; ".to_string());
            assert!(result.is_ok(), "Valid insert query with extra spaces should not return an error");
        }


        #[test]
        fn test_invalid_insert_missing_values() {
            let result = Query::from("INSERT INTO test (col1, col2) (1, 2);".to_string());
            assert!(result.is_err(), "Insert query missing 'values' keyword should return an error");
        }


        #[test]
        fn test_invalid_insert_wrong_order() {
            let result = Query::from("INSERT VALUES (1, 2, 3) INTO test;".to_string());
            assert!(result.is_err(), "Insert query with incorrect syntax should return an error");
        }


        #[test]
        fn test_invalid_select_missing_from() {
            let result = Query::from("SELECT col1, col2 users WHERE age > 25;".to_string());
            assert!(result.is_err(), "Select query missing 'from' keyword should return an error");
        }


        #[test]
        fn test_invalid_select_no_columns() {
            let result = Query::from("SELECT FROM users;".to_string());
            assert!(result.is_err(), "Select query without column list or '*' should return an error");
        }


        #[test]
        fn test_invalid_where_condition_incomplete() {
            let result = Query::from("SELECT * FROM users WHERE age > ;".to_string());
            assert!(result.is_err(), "Select query with incomplete WHERE clause should return an error");
        }


        #[test]
        fn test_valid_delete_with_where() {
            let result = Query::from("DELETE FROM users WHERE age < 18;".to_string());
            assert!(result.is_ok(), "Valid delete query with WHERE clause should not return an error");
        }


        #[test]
        fn test_valid_delete_without_where() {
            let result = Query::from("DELETE FROM users;".to_string());
            assert!(result.is_ok(), "Valid delete query without WHERE clause should not return an error");
        }


        #[test]
        fn test_invalid_delete_missing_from() {
            let result = Query::from("DELETE users WHERE age > 30;".to_string());
            assert!(result.is_err(), "Delete query missing 'FROM' keyword should return an error");
        }


        #[test]
        fn test_invalid_delete_no_table() {
            let result = Query::from("DELETE WHERE age > 30;".to_string());
            assert!(result.is_err(), "Delete query missing table name should return an error");
        }


        #[test]
        fn test_invalid_delete_where_condition_incomplete() {
            let result = Query::from("DELETE FROM users WHERE age = ;".to_string());
            assert!(result.is_err(), "Delete query with incomplete WHERE clause should return an error");
        }


    }


}
