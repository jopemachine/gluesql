use heed::BytesDecode;
use serde::{Serialize, de::DeserializeOwned};

use crate::SerDe;

use {super::HeedStorage, gluesql_core::store::AlterTable};

impl<K: SerDe, V: SerDe> AlterTable for HeedStorage<K, V>
where
    K: SerDe,
    V: SerDe,
    <K as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
    <V as BytesDecode<'static>>::DItem: Serialize + DeserializeOwned,
{
}
