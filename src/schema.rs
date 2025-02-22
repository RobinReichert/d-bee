#![allow(unused)]



use std::{io::Result, path::PathBuf, io::{Error, ErrorKind}, collections::hash_map::HashMap};
use crate::storage::{table_management::{Row, Type, Predicate, Operator, Value, TableHandler, simple::SimpleTableHandler}, file_management::*};



pub struct SchemaHandler {
    table_handler: Box<dyn TableHandler>
}



impl SchemaHandler {


    pub fn new(db_path: &PathBuf) -> Result<SchemaHandler> {
        let path = db_path.join("schema.hive");
        let col_data : Vec<(Type, String)> = vec![(Type::Text, "table_id"), (Type::Text, "col_name"), (Type::Number, "col_type"), (Type::Number, "col_id")].into_iter().map(|(t, n)| (t, n.to_string())).collect();
        let table_handler : Box<dyn TableHandler> = Box::new(SimpleTableHandler::new(path, col_data)?);
        return Ok(SchemaHandler{table_handler});
    }


    pub fn get_col_data(&self, table : String) -> Result<Vec<(Type, String)>> {
        let predicate : Predicate = Predicate{column: "table_id".to_string(), operator: Operator::Equal, value: Value::new_text(table) };
        let res = self.table_handler.select_row(Some(predicate))?;
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


    pub fn get_table_data(&self) -> Result<HashMap<String, Vec<(Type, String)>>> {
        let mut table_data : HashMap<String, Vec<(u64, String, Type)>> = HashMap::new();
        let res = self.table_handler.select_row(None)?;
        if let Some(mut cursor) = res {
            loop {
                let row = cursor.value.clone();
                match (
                    self.table_handler.get_col_from_row(row.clone(), "table_id")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_id")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_name")?,
                    self.table_handler.get_col_from_row(row.clone(), "col_type")?) {
                    (Value::Text(table_id), Value::Number(col_id), Value::Text(col_name), Value::Number(col_type)) => {
                        let col_data : (u64, String, Type) = (col_id, col_name, Type::try_from(col_type)?);
                        if let Some(mut existent) = table_data.insert(table_id.clone(), vec![col_data.clone()]) {
                            table_data.remove(&table_id);
                            existent.push(col_data);
                            table_data.insert(table_id, existent);
                        }
                    },
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "unexpected error cols in schema did not have the right type")),
                }
                if !self.table_handler.next(&mut cursor)? {
                    break;
                }
            }
        }
        let mut end_res : HashMap<String, Vec<(Type, String)>> = HashMap::new();
        for table_id in table_data.keys() {
            let mut col_data = table_data.get(table_id).ok_or_else(|| Error::new(ErrorKind::Other, "unexpected error: key was not found"))?.clone();
            col_data.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
            end_res.insert(table_id.clone(), col_data.into_iter().map(|(_, n, t)| (t, n)).collect());
        }
        return Ok(end_res);
    }


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
        let result = schema_handler.add_col_data(table_name.clone(), col_data.clone());
        assert!(result.is_ok(), "Adding column data should succeed");

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


