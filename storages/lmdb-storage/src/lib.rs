mod alter_table;
pub mod error;
mod function;
mod index;
mod store;
mod transaction;

use {
    async_io::block_on,
    error::{HeedStorageError, OptionExt, ResultExt},
    gluesql_core::{
        ast::ColumnUniqueOption,
        data::{value::HashMapJsonExt, Key, Schema},
        error::{Error, Result},
        store::{DataRow, Metadata, RowIter},
    },
    iter_enum::Iterator,
    serde_json::Value as JsonValue,
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{self, BufRead, Read},
        path::{Path, PathBuf},
    },
};

use gluesql_core::prelude::Value;
use heed::{types::*, RoTxn, RoIter};
use heed::{
    BytesDecode, BytesEncode,
};
use heed::{Env};
use heed::{Database, EnvOpenOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub struct HeedStorage<K, V> {
    pub path: PathBuf,
    pub database_name: String,
    pub env: Env,
    pub db: Option<Database<K, V>>,
}

pub trait SerDe: DeserializeOwned + BytesDecode<'static>
where
    <Self as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
}

impl<K: SerDe, V: SerDe> HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
    pub fn new(path: &str, database_name: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path.to_owned());

        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(path.clone())
            .unwrap();

        Ok(Self {
            env,
            path,
            database_name: database_name.to_owned(),
            db: None,
        })
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema_path = self.schema_path(table_name);
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(&schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                let schema = Schema::from_ddl(&ddl)?;
                if schema.table_name != table_name {
                    return Err(Error::StorageMsg(
                        HeedStorageError::TableNameDoesNotMatchWithFile.to_string(),
                    ));
                }

                schema.column_defs
            }
            false => None,
        };

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            engine: None,
        }))
    }

    fn lmdb_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "lmdb")
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.path_by(table_name, "sql")
    }

    fn path_by(&self, table_name: &str, extension: &str) -> PathBuf {
        let path = self.path.as_path();
        let mut path = path.join(table_name);
        path.set_extension(extension);

        path
    }

    fn scan_data_2(&self, table_name: &str) -> Result<(RowIter, Schema)> {
        let schema = self
            .fetch_schema(table_name)?
            .map_storage_err(HeedStorageError::TableDoesNotExist)?;
    
        let database_name = self.database_name.clone();
    
        let txn = self.env.read_txn().unwrap();
        
        let db: Database<K, V> = self.env
            .open_database(&txn, Some(&database_name))
            .unwrap()
            .unwrap();
    
        let data: Vec<_> = db.iter(&txn).unwrap()
            .map(|item| {
                let item = item.unwrap();
                
                let key_str = serde_json::to_string(&item.0).unwrap();
                let value_str = serde_json::to_string(&item.1).unwrap();
                
                (key_str, value_str)
            })
            .collect();
    
        let it = data.into_iter().map(|(key_str, value_str)| {
            let value: HashMap<String, Value> = serde_json::from_str(&value_str.as_str()).unwrap();
    
            Ok((Key::Str(key_str), DataRow::Map(value)))
        });
    
        Ok((Box::new(it), schema))
    }
}

impl<K, V> Metadata for HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
}
