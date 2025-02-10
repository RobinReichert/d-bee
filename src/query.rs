#![allow(unused)]

pub mod parser {



    use std::{io::{Result, ErrorKind, Error}, collections::hash_map::HashMap};
    use regex::Regex;



    mod bnf {



        use super::*;



        #[derive(Clone, Debug)]
        pub enum Symbol {
            Terminal(String),
            NamedTerminal(String, String),
            Value(String),
            Option(Vec<Symbol>),
            Repeat(Box<Symbol>),
            Sequence(Vec<Symbol>),
        }



        pub fn t(val: &str) -> Symbol {
            return Terminal(val.to_string());
        }



        pub fn nt(val: &str, name: &str) -> Symbol {
            return NamedTerminal(val.to_string(), name.to_string());
        }



        pub fn v(val: &str) -> Symbol {
            return Value(val.to_string());
        }



        pub fn o(os: Vec<Symbol>) ->Symbol {
            return Option(os);
        }



        pub fn r(val: Symbol) -> Symbol {
            return Repeat(Box::new(val));
        }



        pub fn s(mut ss: Vec<Symbol>) ->Symbol {
            ss.reverse();
            return Sequence(ss);
        }



        use Symbol::*;




        pub fn solve(mut stack: Vec<Symbol>,mut input: Vec<String>) -> std::result::Result<HashMap<String, Vec<String>>, (std::io::Error, usize)> {
            if stack.len() == 0 {
                if input.len() > 0 {
                    return Err((Error::new(ErrorKind::InvalidInput, "input was too long"), input.len()));
                }
                return Ok(HashMap::new()); 
            }
            match stack.pop().ok_or_else(|| {(Error::new(ErrorKind::Other, "unexpected: stack was empty"), input.len())})? {
                Terminal(exp) => {
                    let val = String::from(input.pop().ok_or_else(|| {
                        (Error::new(ErrorKind::InvalidInput, "input was too short"), input.len())
                    })?);
                    if exp == val {
                        return solve(stack, input);
                    }
                    return Err((Error::new(ErrorKind::InvalidInput, format!("did not extpect {}, you may want to use {}", val, exp)), input.len()));
                },
                NamedTerminal(exp, id) => {
                    let val = String::from(input.pop().ok_or_else(|| {
                        (Error::new(ErrorKind::InvalidInput, "input was too short"), input.len())
                    })?);
                    if exp == val {
                        let mut res = solve(stack, input)?;
                        if let Some(mut existing) = res.insert(id.clone(), vec![exp.clone()]) {
                            res.remove(&id); 
                            existing.push(exp);
                            res.insert(id, existing);
                        }
                        return Ok(res);
                    }
                    return Err((Error::new(ErrorKind::InvalidInput, format!("did not extpect {}, you may want to use {}", val, exp)), input.len()));
                }
                Value(id) => {
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
                    if let Ok(temp) = solve(stack.clone(), input.clone()) {
                        return Ok(temp);
                    } 
                    stack.push(Sequence(vec![Repeat(symbol.clone()), *symbol]));
                    solve(stack, input)
                }
                Sequence(mut symbols) => {
                    stack.append(&mut symbols);
                    solve(stack, input)
                }
            }
        }



    }



    use bnf::*;



    #[derive(Debug, Clone)]
    pub struct Query {
        plan: HashMap<String, Vec<String>>
    }



    impl Query {


        pub fn from(q: String) -> std::io::Result<Query> {
            let col_names : Symbol = o(vec![s(vec![]), v("column"), s(vec![r(s(vec![v("column"), t(",")])), v("column")])]);
            let col_values : Symbol = o(vec![s(vec![]), v("value"), s(vec![r(s(vec![v("value"), t(",")])), v("value")])]);
            let insert_values : Symbol = o(vec![s(vec![t("("), col_names.clone(), t(")"), t("values"), t("("), col_values.clone(), t(")")]), s(vec![t("values"), t("("), col_values.clone(), t(")")])]);
            let insert : Symbol = s(vec![nt("insert", "command"), t("into"), v("table"), insert_values]);
            let operator : Symbol = o(vec![nt("==", "operator"),nt("!=", "operator"), nt("<", "operator"), nt("<=", "operator"), nt(">", "operator"), nt(">=", "operator")]);
            let predicate : Symbol = o(vec![s(vec![]), s(vec![t("where"), v("predicate_column_name"), operator.clone(), v("predicate_value")])]);
            let columns : Symbol = o(vec![t("*"), v("column"), s(vec![r(s(vec![v("column"), t(",")])), v("column")])]);
            let select : Symbol = s(vec![nt("select", "command"), columns, t("from"), v("table"), predicate.clone()]);
            let delete : Symbol = s(vec![nt("delete", "command"), t("from"), v("table"), predicate.clone()]); 
            let query : Symbol = s(vec![o(vec![insert, select, delete]), t(";")]);
            let regex = Regex::new(r"\w+|[();,*]|(>=)|>|(==)|!=|<|(<=)").unwrap();
            let mut input : Vec<String> = regex.find_iter(&q.to_lowercase()).map(|x| {x.as_str()}).map(|x| {x.to_string()}).collect();
            input.reverse();
            let plan = bnf::solve(vec![query], input).map_err(|e|{Error::new(ErrorKind::InvalidInput, e.0.to_string())});
            return Ok(Query {plan: plan?});
        }


        pub fn execute(&self) -> Result<()>{
            let command = self.plan.get("command").ok_or_else(||{Error::new(ErrorKind::InvalidInput, "query was not valid")})?.first().ok_or_else(||{Error::new(ErrorKind::InvalidInput, "command was empty")})?;
            match command.as_str() {
                "insert" => {
                    Ok(())
                },
                "select" => {
                    Ok(())
                },
                "delete" => {
                    Ok(())
                },
                _ => Err(Error::new(ErrorKind::InvalidInput, "")),
            }
        }


    }



    #[cfg(test)]
    mod test {


        use super::*;


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
            result.unwrap().execute();
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

pub mod schema_management {


    use std::{io::Result, path::PathBuf, io::{Error, ErrorKind}};
    use crate::storage::{table_management::{Row, Type, Predicate, Operator, Value, TableHandler, simple::SimpleTableHandler}, file_management::*};



    pub struct SchemaHandler {
        table_handler: Box<dyn TableHandler>
    }



    impl SchemaHandler {


        pub fn new(db_path: &PathBuf) -> Result<SchemaHandler> {
            let col_data : Vec<(Type, &str)> = vec![(Type::Text, "table_id"), (Type::Text, "col_name"), (Type::Number, "col_type"), (Type::Number, "col_id")];
            let table_handler : Box<dyn TableHandler> = Box::new(SimpleTableHandler::new(db_path.join("schema.hive"), col_data)?);
            return Ok(SchemaHandler{table_handler});
        }


        pub fn get_col_data(&self, table : String) -> Result<Vec<(Type, String)>> {
            let predicate : Predicate = Predicate{column: "table_id".to_string(), operator: Operator::Equal, value: Value::new_text(table) };
            let res = self.table_handler.select_row(predicate)?;
            if let Some(mut cursor) = res {
                let mut col_data : Vec<(u64, String, Type)> = vec![];
                loop {
                    let row = cursor.value.clone();
                    match (
                        self.table_handler.get_col_from_row(row.clone(), "col_id")?,
                        self.table_handler.get_col_from_row(row.clone(), "col_name")?,
                        self.table_handler.get_col_from_row(row.clone(), "col_type")?) {
                        (Value::Number(col_id), Value::Text(col_name), Value::Number(col_type)) => col_data.push((col_id, col_name, Type::try_from(col_type)?)),
                        _ => return Err(Error::new(ErrorKind::InvalidInput, "unexpected error cols in schema did not have the right type")),
                    }
                    if !self.table_handler.next(&mut cursor)? {
                        break;
                    }
                }
                col_data.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
                let end_res : Vec<(Type, String)> = col_data.into_iter().map(|(_, n, t)| (t, n)).collect();
                return Ok(end_res)
            }
            return Ok(vec![]);
        }


        pub fn add_col_data(&self, table : String, data : Vec<(Type, String)>) -> Result<()> {
            for (idx, d) in data.iter().enumerate() {
                let row : Row = Row{cols: vec![Value::new_text(table.clone()), Value::new_text(d.1.clone()), Value::new_number(d.0.clone().into()), Value::new_number(idx as u64)]};
                self.table_handler.insert_row(row)?;
            }
            return Ok(());
        }


    }



    #[cfg(test)]
    mod test {


        use super::*;
        use crate::storage::file_management::{get_test_path, delete_dir};


#[test]
        fn test_schema_handler_creation() {
            let db_path = get_test_path().unwrap();
            let schema_handler = SchemaHandler::new(&db_path);
            assert!(schema_handler.is_ok(), "SchemaHandler should be created successfully");
        }

#[test]
        fn test_add_and_get_col_data() {
            let db_path = get_test_path().unwrap();
            let schema_handler = SchemaHandler::new(&db_path).unwrap();

            let table_name = "test_table".to_string();
            let col_data = vec![(Type::Text, "name".to_string()), (Type::Number, "age".to_string())];

            // Add column data
            let result = schema_handler.add_col_data(table_name.clone(), col_data.clone());
            assert!(result.is_ok(), "Adding column data should succeed");

            // Retrieve column data
            let retrieved_data = schema_handler.get_col_data(table_name).unwrap();
            assert_eq!(retrieved_data, col_data, "Retrieved column data should match inserted data");
        }

#[test]
        fn test_get_col_data_empty() {
            let db_path = get_test_path().unwrap();
            let schema_handler = SchemaHandler::new(&db_path).unwrap();

            let table_name = "non_existent_table".to_string();
            let retrieved_data = schema_handler.get_col_data(table_name);
            assert!(retrieved_data.is_ok(), "Fetching column data for non-existent table should not fail");
            assert!(retrieved_data.unwrap().is_empty(), "Retrieved data should be empty");
        }

    }




}
