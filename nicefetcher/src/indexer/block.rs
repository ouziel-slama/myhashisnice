use pyo3::{
    types::{PyAnyMethods, PyDict, PyBytes},
    IntoPy, PyObject, Python,
};

use super::config::Config;

#[derive(Clone)]
pub struct Vin {
    pub hash: Vec<u8>, // prev output txid
    pub n: u32,       // prev output index
    pub utxo_id: Vec<u8>,
}

impl IntoPy<PyObject> for Vin {
    #[allow(clippy::unwrap_used)]
    fn into_py(self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new_bound(py);
        dict.set_item("hash", PyBytes::new_bound(py, &self.hash))
            .unwrap();
        dict.set_item("n", self.n).unwrap();
        dict.set_item("utxo_id", PyBytes::new_bound(py, &self.utxo_id))
            .unwrap();
        dict.unbind().into()
    }
}

#[derive(Clone)]
pub struct Vout {
    pub value: u64,
    pub address_hash: Vec<u8>,
    pub is_op_return: bool,
    pub n: u64,
    pub utxo_id: Vec<u8>,
}

impl IntoPy<PyObject> for Vout {
    #[allow(clippy::unwrap_used)]
    fn into_py(self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new_bound(py);
        dict.set_item("value", self.value).unwrap();
        dict.set_item("is_op_return", self.is_op_return).unwrap();
        dict.set_item("n", self.n).unwrap();
        dict.set_item("utxo_id", PyBytes::new_bound(py, &self.utxo_id))
            .unwrap();
        dict.set_item("address_hash", PyBytes::new_bound(py, &self.address_hash))
            .unwrap();
        dict.unbind().into()
    }
}


#[derive(Clone)]
pub struct Transaction {
    //pub version: i32,
    //pub segwit: bool,
    pub coinbase: bool,
    //pub lock_time: u32,
    pub tx_id: Vec<u8>,
    //pub tx_hash: String,
    //pub vtxinwit: Vec<Vec<String>>,
    pub vin: Vec<Vin>,
    pub vout: Vec<Vout>,
    pub zero_count: u32,
}




impl IntoPy<PyObject> for Transaction {
    #[allow(clippy::unwrap_used)]
    fn into_py(self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new_bound(py);
        dict.set_item("coinbase", self.coinbase).unwrap();
        dict.set_item("tx_id", PyBytes::new_bound(py, &self.tx_id))
            .unwrap();
        let vin_list: Vec<PyObject> = self.vin.into_iter().map(|vin| vin.into_py(py)).collect();
        dict.set_item("vin", vin_list).unwrap();
        let vout_list: Vec<PyObject> = self.vout.into_iter().map(|vout| vout.into_py(py)).collect();
        dict.set_item("vout", vout_list).unwrap();
        dict.set_item("zero_count", self.zero_count).unwrap();
        dict.unbind().into()
    }
}

#[derive(Clone)]
pub struct Block {
    pub height: u32,
    pub hash_prev: String,
    pub block_time: u32,
    pub block_hash: String,
    pub transaction_count: usize,
    pub transactions: Vec<Transaction>,
    pub max_zero_count: u32,
}

impl IntoPy<PyObject> for Block {
    #[allow(clippy::unwrap_used)]
    fn into_py(self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new_bound(py);
        dict.set_item("height", self.height).unwrap();
        dict.set_item("hash_prev", self.hash_prev).unwrap();
        dict.set_item("block_time", self.block_time).unwrap();
        dict.set_item("block_hash", self.block_hash).unwrap();
        dict.set_item("transaction_count", self.transaction_count)
            .unwrap();

        let transactions_list: Vec<PyObject> = self
            .transactions
            .into_iter()
            .map(|tx| tx.into_py(py))
            .collect();
        dict.set_item("transactions", transactions_list).unwrap();
        dict.set_item("max_zero_count", self.max_zero_count).unwrap();

        dict.unbind().into()
    }
}

pub trait ToBlock {
    fn to_block(&self, config: Config, height: u32) -> Block;
}
