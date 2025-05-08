


    use crate::{schema::TableSchemaHandler, query::parsing::*, storage::{table_management::{Cursor, Operator, Predicate, Row, Type, TableHandler, simple::SimpleTableHandler}, file_management::delete_file}};
    use std::{io::{Result, Error, ErrorKind}, path::PathBuf, collections::hash_map::HashMap, sync::{RwLock, Mutex}};
    use rand::RngCore;



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
            let _ = delete_file(&self.db_path.join(format!("{}.hive", table_name)));             
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
                let _ = handler.insert_row(row);
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

    }




