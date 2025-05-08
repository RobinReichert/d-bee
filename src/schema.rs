#![allow(unused)]



use std::{env, fs::File, io::Result, path::PathBuf, io::{Write, Error, ErrorKind}, collections::hash_map::HashMap, sync::Mutex};
use rand::{Rng, thread_rng};
use dotenv::dotenv;
use crate::storage::{table_management::{Row, Type, Predicate, Operator, Value, TableHandler, simple::SimpleTableHandler}, file_management::*};



pub struct TableSchemaHandler {
    table_handler: Box<dyn TableHandler>
}



impl TableSchemaHandler {

    ///Creates an instance of a TableSchemaHandler. Takes the path of the corresponding database as an
    ///argument.
    pub fn new(db_path: &PathBuf) -> Result<TableSchemaHandler> {

        //Create table at: 
        let path = db_path.join("schema.hive");

        //With cols:
        //Table_id -> represents the table name.
        //Col_name -> represents a col in the table.
        //Col_type -> represents the type of a col as a number that can be decoded by the table management module.
        //Col_id -> this stores the index of a col inside a table in order to order them, since this is important for the creation of a TableHandler.
        let col_data : Vec<(Type, String)> = vec![(Type::Text, "table_id"), (Type::Text, "col_name"), (Type::Number, "col_type"), (Type::Number, "col_id")].into_iter().map(|(t, n)| (t, n.to_string())).collect();
        let table_handler : Box<dyn TableHandler> = Box::new(SimpleTableHandler::new(path, col_data)?);
        return Ok(TableSchemaHandler{table_handler});
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




pub struct DatabaseSchemaHandler {
    table_handler : Box<dyn TableHandler>, 
    databases : Mutex<HashMap<String, String>>,
    admin_key : String,
}



impl DatabaseSchemaHandler {



    pub fn new(base_path: PathBuf) -> Result<Self> {

        //Table containing database_id and database_key is created
        let path = base_path.join("schema.hive");
        let col_data : Vec<(Type, String)> = vec![(Type::Text, "database_id"), (Type::Text, "database_key")].into_iter().map(|(t, n)| (t, n.to_string())).collect();
        let table_handler : Box<dyn TableHandler> = Box::new(SimpleTableHandler::new(path, col_data)?);

        //Map containing database name and key is initialized and filled
        let mut databases : HashMap<String, String> = HashMap::new();
        if let Some((mut value, mut cursor)) = table_handler.select_row(None, None)? {
            loop {
                let database_id : String = table_handler.get_col_from_row(value.clone(), "database_id")?.try_into()?;
                let database_key : String = table_handler.get_col_from_row(value.clone(), "database_key")?.try_into()?;
                databases.insert(database_id, database_key);
                if let Some(new_value) = table_handler.next(&mut cursor)? {
                    value = new_value;
                    continue; 
                }
                break;
            }
        }
        let mut admin_key = String::new();
        let env_path = base_path()?.join(".env");
        if !env_path.exists() { 
            let mut rng = thread_rng();
            for i in (0..32) {
                admin_key.push(rng.gen_range(0x20..=0x7E).into()); 
            }
            let mut file = create_file(&env_path)?;

            // Write some default content
            writeln!(file, "ADMIN_KEY=\"{}\"", admin_key)?;
        }else{
            dotenv::from_path(env_path).map_err(|e| {Error::new(ErrorKind::NotFound, format!("couldnt load env: {}", e))})?;
            admin_key = env::var("ADMIN_KEY").map_err(|e| {Error::new(ErrorKind::NotFound, format!("couldnt find admin key in env file: {}", e))})?;
        }
        return Ok(DatabaseSchemaHandler {table_handler, databases : Mutex::new(databases), admin_key});
    }



    pub fn add_database(&self, database : String, key : String) -> Result<()> {

        //Check if database with this name exists already
        if let Ok(databases) = self.databases.lock() {
            if databases.contains_key(&database) {
                return Err(Error::new(ErrorKind::AlreadyExists, "database does exist already"));
            }
        }

        //Database is added to map and table
        let row : Row = Row{cols: vec![Value::new_text(database.clone()), Value::new_text(key.clone())]};
        self.table_handler.insert_row(row)?;
        if let Ok(mut databases) = self.databases.lock() {
            databases.insert(database, key);
        }
        return Ok(());
    }



    pub fn remove_database(&self, database : String) -> Result<()> {
        if let Ok(mut databases) = self.databases.lock() {
            if databases.remove(&database).is_none() {
                return Err(Error::new(ErrorKind::NotFound, "database does not exist"));
            }
        }
        let predicate = Predicate { column: "database_id".to_string(), operator: Operator::Equal, value: Value::new_text(database.clone())};
        self.table_handler.delete_row(Some(predicate))?;
        return Ok(());
    }



    pub fn get_database_names(&self) -> Result<Vec<String>> {
        if let Ok(databases) = self.databases.lock() {
            return Ok(databases.clone().into_keys().collect()); 
        }
        return Err(Error::new(ErrorKind::Other, "thread poisoned"));
    }



    pub fn get_database_key(&self, database_name : String) -> Result<Option<String>> {
        let databases = self.databases.lock().map_err(|_| Error::new(ErrorKind::Other, "Thread poisoned"))?;
        return Ok(databases.get(&database_name).cloned());
    }



    pub fn check_key(&self, database : String, key : String) -> Result<bool> {
        if let Ok(databases) = self.databases.lock() {
            return match databases.get(&database) {
                Some(val) if *val == key => Ok(true),
                _ => Err(Error::new(ErrorKind::InvalidInput, "wrong key")),
            }
        }
        return Err(Error::new(ErrorKind::Other, "thread poisoned"));
    }



    pub fn check_admin_key(&self, key : String) -> bool {
        return key == self.admin_key; 
    }

}

#[cfg(test)]
mod test {


    use super::*;
    use crate::storage::file_management::{get_test_path, delete_file};


#[test]
    fn table_schema_handler_creation_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = TableSchemaHandler::new(&db_path);
        assert!(schema_handler.is_ok(), "TableSchemaHandler should be created successfully");
    }

#[test]
    fn table_schema_add_and_get_col_data_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = TableSchemaHandler::new(&db_path).unwrap();
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
    fn table_schema_get_col_data_empty_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = TableSchemaHandler::new(&db_path).unwrap();
        let table_name = "non_existent_table".to_string();
        let retrieved_data = schema_handler.get_col_data(table_name);
        assert!(retrieved_data.is_ok(), "Fetching column data for non-existent table should not fail");
        assert!(retrieved_data.unwrap().is_empty(), "Retrieved data should be empty");
    }


#[test]
    fn database_schema_handler_creation_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = DatabaseSchemaHandler::new(get_test_path().unwrap());
        assert!(schema_handler.is_ok(), "TableSchemaHandler should be created successfully");
    }

#[test]
    fn database_schema_add_and_retrieve_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = DatabaseSchemaHandler::new(get_test_path().unwrap()).unwrap();
        let name : String = "bob".to_string();
        let key : String = "key".to_string();
        schema_handler.add_database(name.clone(), key);
        let result = schema_handler.get_database_names();
        assert!(result.is_ok());
        let mut database_names = result.unwrap();
        let result_name = database_names.pop(); 
        assert!(result_name.is_some());
        assert_eq!(name, result_name.unwrap());
    }


    #[test]
    fn database_schma_add_remove_and_retrieve_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = DatabaseSchemaHandler::new(get_test_path().unwrap()).unwrap();
        let name : String = "bob".to_string();
        let key : String = "key".to_string();
        schema_handler.add_database(name.clone(), key);
        schema_handler.remove_database(name.clone());
        let result = schema_handler.get_database_names();
        assert!(result.is_ok());
        let mut database_names = result.unwrap();
        assert_eq!(database_names.len(), 0);
    }


    #[test]
    fn database_schema_get_key_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = DatabaseSchemaHandler::new(get_test_path().unwrap()).unwrap();
        let name : String = "bob".to_string();
        let key : String = "key".to_string();
        schema_handler.add_database(name.clone(), key.clone());
        let result = schema_handler.get_database_key(name);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(key));
    }


#[test]
    fn database_schema_check_key_test() {
        let db_path = get_test_path().unwrap();
        delete_file(&db_path.join("schema.hive"));
        let schema_handler = DatabaseSchemaHandler::new(get_test_path().unwrap()).unwrap();
        let name : String = "bob".to_string();
        let key : String = "key".to_string();
        schema_handler.add_database(name.clone(), key.clone());
        let result = schema_handler.check_key(name, key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }
}

