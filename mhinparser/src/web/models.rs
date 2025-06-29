use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct PaginationQuery {
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl PaginationQuery {
    pub fn validate(&self) -> Result<(u32, u32), String> {
        let page = self.page.unwrap_or(1);
        let limit = self.limit.unwrap_or(20);

        if page == 0 {
            return Err("Page must be greater than 0".to_string());
        }

        if limit == 0 || limit > 1000 {
            return Err("Limit must be between 1 and 1000".to_string());
        }

        Ok((page, limit))
    }

    pub fn offset(&self) -> Result<u32, String> {
        let (page, limit) = self.validate()?;
        Ok((page - 1) * limit)
    }
}

#[derive(Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    pub has_next: bool,
}

impl<T> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, page: u32, limit: u32, total: u64) -> Self {
        let has_next = (page * limit) < total as u32;

        Self {
            data,
            page,
            limit,
            total,
            has_next,
        }
    }
}

#[derive(Serialize)]
pub struct NiceHashResponse {
    pub txid: String,
    pub block_height: u64,
    pub reward: u64,
    pub zero_count: u64,
}

#[derive(Serialize)]
pub struct AddressBalanceResponse {
    pub address: String,
    pub total_balance: u64,
    pub utxo_count: usize,
    pub utxos: HashMap<String, u64>,
}

#[derive(Deserialize)]
pub struct MempoolUtxo {
    pub txid: String,
    pub vout: u32,
    pub value: u64,
}

#[derive(Serialize)]
pub struct StatsResponse {
    pub stats: HashMap<String, String>,
}

#[derive(Deserialize)]
pub struct ComposeQuery {
    pub source: String,
    pub fee_rate: Option<u64>,
    // JSON encoded: [{"address": "...", "amount": 123}, ...]
    pub outputs: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MhinOutputData {
    pub address: String,
    pub amount: u64, // MHIN amount in satoshis (1 MHIN = 100_000_000 sats)
}

#[derive(Serialize)]
pub struct ComposeResponse {
    pub psbt_hex: String,
    pub source: String,
    pub outputs: Vec<MhinOutputData>,
    pub fee_rate: u64,
}