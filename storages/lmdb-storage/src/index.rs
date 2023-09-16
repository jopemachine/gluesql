use heed::BytesDecode;
use serde::{Serialize, de::DeserializeOwned};

use crate::SerDe;

use {
    super::HeedStorage,
    gluesql_core::store::{Index, IndexMut},
};

impl<K: SerDe, V: SerDe> Index for HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
}
impl<K: SerDe, V: SerDe> IndexMut for HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
}
