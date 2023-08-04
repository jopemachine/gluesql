use error::{ExecuteError, ParsingError, PlanError, TranslateError};
use payload::convert;

use gluesql_core::{
    ast::Statement,
    prelude::{execute, parse, plan, Payload},
    translate::translate,
};
use pyo3::{prelude::*, types::PyString};
use storages::{PyJsonStorage, PyMemoryStorage, PySharedMemoryStorage, PySledStorage};
mod error;
mod payload;
mod storages;

#[derive(FromPyObject)]
pub enum PyStorageEngine {
    MemoryStorage(PyMemoryStorage),
    JsonStorage(PyJsonStorage),
    SharedMemoryStorage(PySharedMemoryStorage),
    SledStorage(PySledStorage),
}

#[pyclass(name = "Glue")]
pub struct PyGlue {
    pub storage: Option<PyStorageEngine>,
}

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

impl PyGlue {
    #[tokio::main]
    pub async fn plan(&self, statement: Statement) -> Result<Statement, PyErr> {
        let storage = self.storage.as_ref().unwrap();

        match storage {
            PyStorageEngine::MemoryStorage(storage) => {
                let memory_storage = storage.0.clone();
                plan(&memory_storage, statement)
                    .await
                    .map_err(|e| PlanError::new_err(e.to_string()))
            }
            PyStorageEngine::JsonStorage(storage) => {
                let memory_storage = storage.0.clone();
                plan(&memory_storage, statement)
                    .await
                    .map_err(|e| PlanError::new_err(e.to_string()))
            }
            PyStorageEngine::SharedMemoryStorage(storage) => {
                let memory_storage = storage.0.clone();
                plan(&memory_storage, statement)
                    .await
                    .map_err(|e| PlanError::new_err(e.to_string()))
            }
            PyStorageEngine::SledStorage(storage) => {
                let memory_storage = storage.0.clone();
                plan(&memory_storage, statement)
                    .await
                    .map_err(|e| PlanError::new_err(e.to_string()))
            }
        }
    }

    #[tokio::main]
    pub async fn execute(&mut self, statement: Statement) -> Result<Payload, PyErr> {
        let storage = self.storage.as_mut().unwrap();

        match storage {
            PyStorageEngine::MemoryStorage(memory_storage) => {
                execute(&mut memory_storage.0, &statement)
                    .await
                    .map_err(|e| ExecuteError::new_err(e.to_string()))
            }
            PyStorageEngine::JsonStorage(json_storage) => execute(&mut json_storage.0, &statement)
                .await
                .map_err(|e| ExecuteError::new_err(e.to_string())),
            PyStorageEngine::SharedMemoryStorage(shared_memory_storage) => {
                execute(&mut shared_memory_storage.0, &statement)
                    .await
                    .map_err(|e| ExecuteError::new_err(e.to_string()))
            }
            PyStorageEngine::SledStorage(sled_storage) => {
                execute(&mut sled_storage.0, &statement)
                    .await
                    .map_err(|e| ExecuteError::new_err(e.to_string()))
            }
        }
    }
}

#[pymethods]
impl PyGlue {
    #[new]
    pub fn new() -> Self {
        PyGlue { storage: None }
    }

    pub fn set_default_engine(&mut self, default_engine: PyStorageEngine) {
        self.storage = Some(default_engine);
    }

    pub fn query(&mut self, py: Python, sql: &PyString) -> PyResult<PyObject> {
        let sql = sql.to_string();
        let queries = parse(&sql).map_err(|e| ParsingError::new_err(e.to_string()))?;

        let mut payloads: Vec<PyPayload> = vec![];
        for query in queries.iter() {
            let statement = translate(query).map_err(|e| TranslateError::new_err(e.to_string()))?;
            let statement = self.plan(statement)?;

            let payload = self.execute(statement)?;

            payloads.push(PyPayload { payload });
        }

        Ok(convert(py, payloads))
    }
}

#[pymodule]
fn gluesql(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyGlue>()?;
    m.add_class::<PyMemoryStorage>()?;
    m.add_class::<PyJsonStorage>()?;
    m.add_class::<PySharedMemoryStorage>()?;
    Ok(())
}
