#![allow(unused)]



use std::{io::Result, path::PathBuf, io::{Error, ErrorKind}, collections::hash_map::HashMap};
use crate::storage::{table_management::{Row, Type, Predicate, Operator, Value, TableHandler, simple::SimpleTableHandler}, file_management::*};



pub struct SchemaHandler {
    table_handler: Box<dyn TableHandler>
}



impl SchemaHandler {

    ///Creates an instance of a SchemaHandler. Takes the path of the corresponding database as an
    ///argument.
    pub fn new(db_path: &PathBuf) -> Result<SchemaHandler> {

        //Create table at: 
        let path = db_path.join("schema.hive");

        //With cols:
        //Table_id -> represents the table name.
        //Col_name -> represents a col in the table.
        //Col_type -> represents the type of a col as a number that can be decoded by the table management module.
        //Col_id -> this stores the index of a col inside a table in order to order them, since this is important for the creation of a TableHandler.
        let col_data : Vec<(Type, String)> = vec![(Type::Text, "table_id"), (Type::Text, "col_name"), (Type::Number, "col_type"), (Type::Number, "col_id")].into_iter().map(|(t, n)| (t, n.to_string())).collect();
        let table_handler : Box<dyn TableHandler> = Box::new(SimpleTableHandler::new(path, col_data)?);
        return Ok(SchemaHandler{table_handler});
    }

    ///Collects data of one table and then returns the cols. Takes the table name that should be
    ///searched for as an argument.
    pub fn get_col_data(&self, table : String) -> Result<Vec<(Type, String)>> {

        //Query the table for rows that match the table name.
        let predicate : Predicate = Predicate{column: "table_id".to_string(), operator: Operator::Equal, value: Value::new_text(table) };
        let res = self.table_handler.select_row(Some(predicate), None)?;

        //Error check query result.
        if let Some((mut value, mut cursor)) = res {
            let mut col_data : Vec<(u64, String, Type)> = vec![];
            loop {
                let row = value.clone();
                match (
                    self.table_handler.get_col_from_row(row.clone(), "col_id")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_name")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_type")?) {
                    (Value::Number(col_id), Value::Text(col_name), Value::Number(col_type)) => col_data.push((col_id, col_name, Type::try_from(col_type)?)),
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "unexpected error cols in schema did not have the right type")),
                }
                if let Some(r) = self.table_handler.next(&mut cursor)? {
                    value = r;
                }else{
                    break;
                }
            }

            //Sort cols by col_id
            col_data.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
            let end_res : Vec<(Type, String)> = col_data.into_iter().map(|(_, n, t)| (t, n)).collect();
            return Ok(end_res)
        }
        return Ok(vec![]);
    }

    
    ///Adds a column to the schema. This column can then be retrieved by get table data or get col
    ///data
    pub fn add_col_data(&self, table : String, col : (Type, String)) -> Result<()> {
        let predicate : Predicate = Predicate{column: "table_id".to_string(), operator: Operator::Equal, value: Value::new_text(table.clone())};
        let mut index = 0;
        if let Some((mut value, mut cursor)) = self.table_handler.select_row(Some(predicate), None)? {
            loop{
                index += 1;
                if value.cols.iter().any(|n| Value::Text(col.1.clone()) == *n) {
                    return Err(Error::new(ErrorKind::AlreadyExists, "col already exists in table"));
                }
                if let Some(row) = self.table_handler.next(&mut cursor)? {
                    value = row;
                }else{
                    break;
                }
            }
        }
        let row : Row = Row{cols: vec![Value::new_text(table.clone()), Value::new_text(col.1.clone()), Value::new_number(col.0.clone().into()), Value::new_number(index as u64)]};
        self.table_handler.insert_row(row)?;
        return Ok(());
    }

    ///Returns the data of all tables as a map with keys of table names and values containing a vec of
    ///Columns. 
    pub fn get_table_data(&self) -> Result<HashMap<String, Vec<(Type, String)>>> {
        let mut table_data : HashMap<String, Vec<(u64, String, Type)>> = HashMap::new();

        //Query the table without a predicate and thereby get all cols.
        let res = self.table_handler.select_row(None, None)?;
        if let Some((mut value, mut cursor)) = res {
            loop {
                let row = value.clone();
                
                //Check result for errors
                match (
                    self.table_handler.get_col_from_row(row.clone(), "table_id")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_id")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_name")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_type")?) {
                    (Value::Text(table_id), Value::Number(col_id), Value::Text(col_name), Value::Number(col_type)) => {
                        let col_data : (u64, String, Type) = (col_id, col_name, Type::try_from(col_type)?);

                        //Insert col into table value or create new key value pair if necessary
                        if let Some(mut existent) = table_data.insert(table_id.clone(), vec![col_data.clone()]) {
                            table_data.remove(&table_id);
                            existent.push(col_data);
                            table_data.insert(table_id, existent);
                        }
                    },
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "unexpected error cols in schema did not have the right type")),
                }
                if let Some(r) = self.table_handler.next(&mut cursor)? {
                    value = r;
                }else{
                    break;
                }
            }
        }

        //Sort all tables columns
        let mut end_res : HashMap<String, Vec<(Type, String)>> = HashMap::new();
        for table_id in table_data.keys() {
            let mut col_data = table_data.get(table_id).ok_or_else(|| Error::new(ErrorKind::Other, "unexpected error: key was not found"))?.clone();
            col_data.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
            end_res.insert(table_id.clone(), col_data.into_iter().map(|(_, n, t)| (t, n)).collect());
        }
        return Ok(end_res);
    }

    
    ///Remove a tables entries from the Schema
    pub fn remove_table_data(&self, table : String) -> Result<()> {
        let predicate : Predicate = Predicate{column: "table_id".to_string(), operator: Operator::Equal, value: Value::new_text(table) };
        return self.table_handler.delete_row(Some(predicate));
    }


}



#[cfg(test)]
mod test {


    use super::*;
    use crate::storage::file_management::{get_test_path, delete_file};


#[test]
    fn test_schema_handler_creation() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = SchemaHandler::new(&db_path);
        assert!(schema_handler.is_ok(), "SchemaHandler should be created successfully");
    }

#[test]
    fn test_add_and_get_col_data() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = SchemaHandler::new(&db_path).unwrap();
        let table_name = "test_table".to_string();
        let col_data = vec![(Type::Text, "name".to_string()), (Type::Number, "age".to_string())];

        // Add column data
        for col in col_data.clone() {
            let result = schema_handler.add_col_data(table_name.clone(), col);
        assert!(result.is_ok(), "Adding column data should succeed");
        }

        // Retrieve column data
        let retrieved_data = schema_handler.get_col_data(table_name).unwrap();
        assert_eq!(retrieved_data, col_data, "Retrieved column data should match inserted data");
    }

#[test]
    fn test_get_col_data_empty() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = SchemaHandler::new(&db_path).unwrap();

        let table_name = "non_existent_table".to_string();
        let retrieved_data = schema_handler.get_col_data(table_name);
        assert!(retrieved_data.is_ok(), "Fetching column data for non-existent table should not fail");
        assert!(retrieved_data.unwrap().is_empty(), "Retrieved data should be empty");
    }

}


