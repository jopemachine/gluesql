use std::sync::Arc;

use heed::BytesDecode;
use serde::{Serialize, de::DeserializeOwned};

use crate::SerDe;

use {
    crate::{
        error::{HeedStorageError, OptionExt, ResultExt},
        HeedStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, RowIter, Store},
    },
    std::{ffi::OsStr, fs},
};

#[async_trait(?Send)]
impl<K: SerDe, V: SerDe> Store for HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
        let mut schemas = paths
            .filter(|result| match result {
                Ok(entry) => {
                    let path = entry.path();
                    let extension = path.extension().and_then(OsStr::to_str);

                    extension == Some("lmdb")
                }
                Err(_) => true,
            })
            .map(|result| -> Result<_> {
                let path = result.map_storage_err()?.path();
                let table_name = path
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map_storage_err(HeedStorageError::FileNotFound)?;

                self.fetch_schema(table_name)?
                    .map_storage_err(HeedStorageError::TableDoesNotExist)
            })
            .collect::<Result<Vec<Schema>>>()?;

        schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let s = self.clone();
        let k = s.scan_data(table_name);
        let k = s.scan_data(table_name).await.unwrap();

        for item in k {
            let (key, row) = item?;

            if &key == target {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let arc_self = Arc::new(self.clone());
        
        // `Arc`를 이용해 안전하게 클론한 `self`의 참조를 얻습니다.
        let cloned_self = arc_self.clone();

        // 이제 `cloned_self`를 사용하여 다른 메서드를 호출하거나 필요한 작업을 수행합니다.
        // 예를 들어:
        let k = cloned_self.scan_data_2(table_name).unwrap();
        Ok(k.0)
    }
}
