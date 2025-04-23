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



pub mod execution {



    use super::parsing::*;
    use crate::{schema::TableSchemaHandler, storage::{table_management::{Cursor, Operator, Predicate, Row, Type, TableHandler, simple::SimpleTableHandler}, file_management::delete_file}};
    use std::{io::{Result, Error, ErrorKind}, path::PathBuf, collections::hash_map::HashMap, sync::{RwLock, Mutex}};
    use rand::RngCore;
    use hex::encode;



    pub struct Executor {
        db_path : PathBuf,
        schema : TableSchemaHandler,
        tables : RwLock<Vec<(String, Box<dyn TableHandler>)>>,

        //Map that maps a hash to a cursor so requests can access a cursor via the hash
        cursors : Mutex<HashMap<Vec<u8>, (String, Cursor)>>,
    }



    impl Executor {


        pub fn new(db_path: PathBuf) -> Result<Self> {
            let schema : TableSchemaHandler = TableSchemaHandler::new(&db_path)?;

            //Fill tables with Table Handlers constructed with data from the schema
            let mut tables : Vec<(String, Box<dyn TableHandler>)> = vec![];
            let table_data = schema.get_table_data()?;
            for table_id in table_data.keys() {
                tables.push((table_id.clone(), Box::new(SimpleTableHandler::new(db_path.join(format!("{}.hive", table_id)), table_data.get(table_id).ok_or_else(|| Error::new(ErrorKind::Other, "unexpected error when creating new Executor"))?.clone())?)));
            }
            let cursors = Mutex::new(HashMap::new());
            return Ok(Executor{db_path, schema, tables: RwLock::new(tables), cursors});
        }


        ///Used to create a new table in the database
        fn create(&self, args : HashMap<String, Vec<String>>) -> Result<()> {

            //Extract table name from the args map
            let table_name : String = args.get(TABLE_NAME_KEY).ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain a table name")})?.first().ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain a table name")})?.clone();

            //Check if table does exist
            if let Ok(tables) = self.tables.write() {
                if tables.iter().any(|(t, _)| *t == table_name) {
                    return Err(Error::new(ErrorKind::InvalidInput, "table exists already"));
                }
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }

            //Extract information about the tables columns
            let col_types : Vec<String> = args.get(COLUMN_TYPE_KEY).ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain col types")})?.clone();
            let col_names : Vec<String> = args.get(COLUMN_NAME_KEY).ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain col names")})?.clone();
            if col_types.len() != col_names.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "args col types and col names had different lengths"));
            }

            //Combine column information
            let mut col_data : Vec<(Type, String)> = vec![];
            for i in 0..col_types.len() {
                col_data.push((Type::try_from(col_types[i].clone())?, col_names[i].clone()));
            }

            //Construct new TableHandler
            let new_table = Box::new(SimpleTableHandler::new(self.db_path.join(format!("{}.hive", table_name)), col_data.clone())?);

            //Insert new TableHandler into tables vec
            if let Ok(mut tables) = self.tables.write() {
                tables.push((table_name.clone(), new_table));
                for col in col_data {
                    self.schema.add_col_data(table_name.clone(), col)?;
                }
                return Ok(());
            }else {
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }
        }


        ///Used to delete a whole table
        fn drop(&self, args : HashMap<String, Vec<String>>) -> Result<()> {

            //Extract table name from args map
            let table_name : String = args.get(TABLE_NAME_KEY).ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain a table name")})?.first().ok_or_else(||{Error::new(ErrorKind::InvalidInput, "args did not contain a table name")})?.clone();

            //Check if table exists
            if let Ok(tables) = self.tables.read() {
                if !tables.iter().any(|(t, _)|*t == table_name) {
                    return Err(Error::new(ErrorKind::InvalidInput, "table does not exists"));
                }
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }

            //Remove TableHandler from memory
            self.schema.remove_table_data(table_name.clone())?;
            if let Ok(mut tables) = self.tables.write() {
                tables.retain(|(n, _)| *n != table_name.clone()); 
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }

            //Clean up used file
            delete_file(&self.db_path.join(format!("{}.hive", table_name)));             
            return Ok(());
        }


        ///Inserts a row into a table
        fn insert(&self, args : HashMap<String, Vec<String>>) -> Result<()> {

            //Extract table name from args map
            let table_name : String = args.get(TABLE_NAME_KEY).ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.first().ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.clone();

            //Extract row data from args map
            let col_names_option : Option<Vec<String>> = args.get(COLUMN_NAME_KEY).cloned();
            let col_values : Vec<String> = args.get(COLUMN_VALUE_KEY).ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain col values"))?.clone();
            if let Some(ref col_names) = col_names_option {
                if col_names.len() != col_values.len() {
                    return Err(Error::new(ErrorKind::InvalidInput, "amount of values and columns did not match"));
                }
            }

            //Choose the table handler and use it to insert the row into the table
            if let Ok(tables) = self.tables.read() {
                let handler = &tables.iter().find(|(t, _)| *t== table_name).ok_or_else(||Error::new(ErrorKind::InvalidInput, "table does not exist"))?.1;
                let row = handler.cols_to_row(col_names_option, col_values)?;
                handler.insert_row(row);
                return Ok(());
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }
        }


        ///Selects a row from a table
        fn select(&self, args : HashMap<String, Vec<String>>) -> Result<Option<(Vec<u8>, Row)>> {

            //Extract table name
            let table_name : String = args.get(TABLE_NAME_KEY).ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.first().ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.clone();

            //Extract the columns that should be returned
            let col_names : Option<Vec<String>> = args.get(COLUMN_NAME_KEY).cloned();
            if let Ok(tables) = self.tables.read() {

                //Check if table exists and get it if possible
                let handler = &tables.iter().find(|(t, _)| *t== table_name).ok_or_else(||Error::new(ErrorKind::InvalidInput, "table does not exist"))?.1;

                //Construct predicate from args
                let predicate : Option<Predicate> = match (
                    args.get(PREDICATE_COL),
                    args.get(OPERATOR_KEY),
                    args.get(PREDICATE_VAL),
                ) {
                    (Some(column), Some(operator), Some(value)) => {
                        match (
                            column.first(),
                            operator.first(),
                            value.first(),
                        ){
                            (Some(column), Some(operator), Some(value)) => {
                                let operator = Operator::try_from(operator.clone())?;
                                let value = handler.create_value(column.clone(), value.clone())?;
                                Some(Predicate{column : column.clone(), operator, value})
                            },

                            //If there is no predicate in args the query is executed without one
                            _ => None,
                        }
                    },
                    _ => None,
                };

                //Execute the query
                Ok(match handler.select_row(predicate, col_names)? {
                    Some((r, c)) => {

                        //Store the cursor in the cursors map along with a randomly generated hash
                        let mut hash = [0u8; 16];  
                        loop {
                            rand::thread_rng().fill_bytes(&mut hash);
                            if let Ok(mut cursors) = self.cursors.lock() {
                                if cursors.contains_key(&hash.to_vec()) {
                                    continue;
                                }
                                cursors.insert(hash.to_vec(), (table_name, c));
                                break;
                            }else{
                                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
                            }
                        }

                        //Return the hash as a pointer to the cursor and the row
                        Some((hash.to_vec(), r))
                    },
                    None => None,
                })
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }
        }


        ///Used to delete rows from a table that match a certain predicate
        fn delete(&self, args : HashMap<String, Vec<String>>) -> Result<()> {

            //Extract table name from args
            let table_name : String = args.get(TABLE_NAME_KEY).ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.first().ok_or_else(||Error::new(ErrorKind::InvalidInput, "args did not contain a table name"))?.clone();

            //Create predicate from args
            if let Ok(tables) = self.tables.read() {
                let handler = &tables.iter().find(|(t, _)| *t== table_name).ok_or_else(||Error::new(ErrorKind::InvalidInput, "table does not exist"))?.1;
                let predicate : Option<Predicate> = match (
                    args.get(PREDICATE_COL),
                    args.get(OPERATOR_KEY),
                    args.get(PREDICATE_VAL),
                ) {
                    (Some(column), Some(operator), Some(value)) => {
                        match (
                            column.first(),
                            operator.first(),
                            value.first(),
                        ){
                            (Some(column), Some(operator), Some(value)) => {
                                let operator = Operator::try_from(operator.clone())?;
                                let value = handler.create_value(column.clone(), value.clone())?;
                                Some(Predicate{column : column.clone(), operator, value})
                            },
                            _ => None,
                        }
                    },
                    _ => None,
                };

                //Delete rows
                Ok(handler.delete_row(predicate)?)
            }else{
                return Err(Error::new(ErrorKind::Other, "thread poisoned"));
            }
        }


        ///Like select but with a starting point
        pub fn next(&self, hash : Vec<u8>) -> Result<Option<Row>> {
            match (self.tables.read(), self.cursors.lock()) {
                (Ok(tables), Ok(mut cursors)) => {

                    //Get the cursor corresponding to the hash
                    let (table_name, cursor) = cursors.get_mut(&hash).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "hash is invalid"))?;

                    //Try to access the table stored with the cursor
                    let handler = &tables.iter().find(|(t, _)| *t==*table_name).ok_or_else(||Error::new(ErrorKind::InvalidInput, "table does not exist"))?.1;

                    //Get next
                    handler.next(cursor)},
                _ => Err(Error::new(ErrorKind::Other, "thread poisoned")),
            }
        }


        pub fn execute(&self, query: Query) -> Result<Option<(Vec<u8>, Row)>>{

            //Extract the command token from the input
            let command = query.plan.get(COMMAND_KEY).ok_or_else(||{Error::new(ErrorKind::InvalidInput, "query was not valid")})?.first().ok_or_else(||{Error::new(ErrorKind::InvalidInput, "command was empty")})?;

            //Execute an action according to that token
            Ok(match command.as_str() {
                CREATE => {
                    self.create(query.plan.clone())?;
                    None
                },
                DROP => {
                    self.drop(query.plan.clone())?;
                    None
                },
                INSERT => {
                    self.insert(query.plan.clone())?;
                    None
                },
                SELECT => {
                    self.select(query.plan.clone())?
                },
                DELETE => {
                    self.delete(query.plan.clone())?;
                    None
                },
                _ => return Err(Error::new(ErrorKind::InvalidInput, ""))

            })
        }

    }


    #[cfg(test)]
    pub mod test {


        use super::*;
        use crate::storage::file_management::{get_test_path, delete_file};


        #[test]
        fn test_valid_create_table() {
            let q = Query::from("CREATE TABLE test_table (test_col TEXT);".to_string()).unwrap();
            let q2 = Query::from("DROP TABLE test_table;".to_string()).unwrap();
            let q3 = Query::from("CREATE TABLE test_table3 (test_col TEXT);".to_string()).unwrap();
            let q4 = Query::from("INSERT INTO test_table (test_col) VALUES (hallo);".to_string()).unwrap();
            let q6 = Query::from("INSERT INTO test_table (test_col) VALUES (welt);".to_string()).unwrap();
            let q5 = Query::from("SELECT * FROM test_table;".to_string()).unwrap();
            let db_path = get_test_path().unwrap();
            let mut e = Executor::new(db_path).unwrap();
            e.execute(q).unwrap();
            e.execute(q3).unwrap();
            e.execute(q4).unwrap();
            e.execute(q6).unwrap();
            let res = e.execute(q5).unwrap();
            if let Some(mut cursor) = res {
            }
            e.execute(q2).unwrap();
        }


    }



}
