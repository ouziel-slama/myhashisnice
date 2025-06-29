use crate::block::generate_utxo_id;
use crate::store::utils::inverse_hash;
use crate::store::MhinStore;
use crate::web::errors::{ApiError, ApiResult};
use crate::web::models::{
    AddressBalanceResponse, MempoolUtxo, NiceHashResponse, PaginatedResponse, PaginationQuery,
    StatsResponse,
};

use actix_web::{web, HttpResponse, Result};
use bitcoin::Address;
use hex::FromHex;
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tera::{Context, Tera};

pub struct AppState {
    pub mhin_store: Arc<MhinStore>,
    pub mempool_client: MempoolClient,
    pub tera: Tera,
}

pub struct MempoolClient {
    client: Client,
    base_url: String,
}

impl MempoolClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: "https://mempool.space/api".to_string(),
        }
    }

    pub async fn get_address_utxos(&self, address: &str) -> ApiResult<Vec<MempoolUtxo>> {
        let url = format!("{}/address/{}/utxo", self.base_url, address);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ApiError::ExternalApi(format!("Failed to fetch UTXOs: {}", e)))?;

        if !response.status().is_success() {
            return Err(ApiError::ExternalApi(format!(
                "Mempool API returned status: {}",
                response.status()
            )));
        }

        let utxos: Vec<MempoolUtxo> = response
            .json()
            .await
            .map_err(|e| ApiError::ExternalApi(format!("Failed to parse UTXOs response: {}", e)))?;

        Ok(utxos)
    }
}

// Helper functions for HTML display
fn bold_zero(value: &str) -> String {
    if !value.starts_with('0') {
        return value.to_string();
    }

    let mut bold_value = String::from("<b>");
    let mut closed = false;

    for char in value.chars() {
        if char != '0' && !closed {
            bold_value.push_str("</b>");
            bold_value.push(char);
            closed = true;
        } else {
            bold_value.push(char);
        }
    }

    bold_value
}

fn display_utxo(txid: &str, full: bool) -> String {
    let txid = inverse_hash(txid);
    let bolded = if full {
        bold_zero(&txid)
    } else {
        let short_txid = format!("{}...{}", &txid[..12], &txid[txid.len() - 12..]);
        bold_zero(&short_txid)
    };
    format!(
        r#"<a href="https://mempool.space/tx/{}" target="_blank">{}</a>"#,
        txid, bolded
    )
}

fn display_quantity(quantity: u64) -> String {
    let value = quantity as f64 / 100_000_000.0; // Convert satoshis to MHIN

    // Format with 8 decimal places
    let formatted = format!("{:.8}", value);

    // Split the integer and decimal parts
    let parts: Vec<&str> = formatted.split('.').collect();
    let integer_part = parts[0];
    let decimal_part = parts.get(1).map_or("", |v| v);

    // Add thousand separators to integer part
    let formatted_integer = if integer_part == "0" {
        "0".to_string()
    } else {
        integer_part
            .chars()
            .rev()
            .collect::<Vec<char>>()
            .chunks(3)
            .map(|chunk| chunk.iter().collect::<String>())
            .collect::<Vec<String>>()
            .join(",")
            .chars()
            .rev()
            .collect()
    };

    // Remove trailing zeros from decimal part
    let trimmed_decimal = decimal_part.trim_end_matches('0');

    // Combine parts
    let final_number = if trimmed_decimal.is_empty() {
        formatted_integer
    } else {
        format!("{}.{}", formatted_integer, trimmed_decimal)
    };

    format!("{} MHIN", final_number)
}

fn format_number(num: u64) -> String {
    num.to_string()
        .chars()
        .rev()
        .collect::<Vec<char>>()
        .chunks(3)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect::<Vec<String>>()
        .join(",")
        .chars()
        .rev()
        .collect()
}

// HTML Page Handlers
pub async fn home_page(app_state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let (nicehash_data, _) = app_state
        .mhin_store
        .get_nicehashes_paginated(10, 0)
        .map_err(ApiError::Database)?;

    let rewards: Vec<serde_json::Value> = nicehash_data
        .into_iter()
        .map(|(height, txid, _zero_count, reward)| {
            json!({
                "height": height,
                "txid": display_utxo(&inverse_hash(&txid), true),
                "reward": display_quantity(reward)
            })
        })
        .collect();

    let stats_data = app_state.mhin_store.get_all_stats();
    let stats = json!({
        "last_parsed_block": stats_data.get("block_height").unwrap_or(&"0".to_string()),
        "supply": display_quantity(
            stats_data.get("mhin_supply").unwrap_or(&"0".to_string()).parse::<u64>().unwrap_or(0)
        ),
        "nice_hashes_count": format_number(
            stats_data.get("nice_hashes_count").unwrap_or(&"0".to_string()).parse::<u64>().unwrap_or(0)
        ),
        "nicest_hash": stats_data.get("nicest_hash")
            .map(|h| display_utxo(&inverse_hash(h), false))
            .unwrap_or_default(),
        "last_nice_hash": stats_data.get("last_nice_hash")
            .map(|h| display_utxo(&inverse_hash(h), false))
            .unwrap_or_default(),
        "first_nice_hash": stats_data.get("first_nice_hash")
            .map(|h| display_utxo(&inverse_hash(h), false))
            .unwrap_or_default(),
        "utxos_count": format_number(
            stats_data.get("unspent_utxo_id_count").unwrap_or(&"0".to_string()).parse::<u64>().unwrap_or(0)
        ),
        "spent_utxos_count": format_number(
            stats_data.get("spent_utxo_id_count").unwrap_or(&"0".to_string()).parse::<u64>().unwrap_or(0)
        ),
    });

    let mut context = Context::new();
    context.insert("rewards", &rewards);
    context.insert("stats", &stats);

    let html = app_state
        .tera
        .render("home.html", &context)
        .map_err(|e| ApiError::Template(e.to_string()))?;

    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

pub async fn protocol_page(app_state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let context = Context::new();

    let html = app_state
        .tera
        .render("protocol.html", &context)
        .map_err(|e| ApiError::Template(e.to_string()))?;

    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

pub async fn balances_page(
    app_state: web::Data<AppState>,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, ApiError> {
    let mut context = Context::new();

    if let Some(address_str) = query.get("address") {
        // Validate Bitcoin address
        let _address = Address::from_str(address_str).map_err(|_| {
            ApiError::InvalidAddress(format!("Invalid Bitcoin address: {}", address_str))
        })?;

        // Get UTXOs from mempool.space
        let utxos = app_state
            .mempool_client
            .get_address_utxos(address_str)
            .await?;

        let mut total_balance = 0u64;
        let mut utxos_list: Vec<(String, String)> = Vec::new();

        // For each UTXO, calculate utxo_id and get balance from store
        for utxo in &utxos {
            let txid_bytes = Vec::from_hex(inverse_hash(&utxo.txid)).map_err(|e| {
                ApiError::ExternalApi(format!("Invalid txid format from mempool: {}", e))
            })?;

            if txid_bytes.len() != 32 {
                return Err(ApiError::ExternalApi(
                    "Invalid txid length from mempool".to_string(),
                ));
            }

            let utxo_id = generate_utxo_id(&txid_bytes, utxo.vout as u64);

            if let Some(balance) = app_state.mhin_store.get_balance(utxo_id) {
                total_balance += balance;
                utxos_list.push((
                    display_utxo(&inverse_hash(&utxo.txid), true),
                    display_quantity(balance),
                ));
            }
        }

        let balance = json!({
            "total_balance": display_quantity(total_balance),
            "utxos": utxos_list
        });

        context.insert("address", address_str);
        context.insert("balance", &balance);
    }

    let html = app_state
        .tera
        .render("balances.html", &context)
        .map_err(|e| ApiError::Template(e.to_string()))?;

    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

// API Handlers (existing)
pub async fn get_nicehashes(
    app_state: web::Data<AppState>,
    query: web::Query<PaginationQuery>,
) -> Result<HttpResponse, ApiError> {
    let (page, limit) = query.validate().map_err(ApiError::InvalidPagination)?;
    let offset = query.offset().map_err(ApiError::InvalidPagination)?;

    let (nicehash_data, total) = app_state
        .mhin_store
        .get_nicehashes_paginated(limit, offset)
        .map_err(|e| ApiError::Database(e))?;

    let nicehashes: Vec<NiceHashResponse> = nicehash_data
        .into_iter()
        .map(
            |(block_height, txid, zero_count, reward)| NiceHashResponse {
                block_height,
                txid,
                zero_count,
                reward,
            },
        )
        .collect();

    let response = PaginatedResponse::new(nicehashes, page, limit, total);
    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_stats(app_state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let stats = app_state.mhin_store.get_all_stats();
    let response = StatsResponse { stats };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn get_address_balance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let address_str = path.into_inner();

    // Validate Bitcoin address
    let _address = Address::from_str(&address_str).map_err(|_| {
        ApiError::InvalidAddress(format!("Invalid Bitcoin address: {}", address_str))
    })?;

    // Get UTXOs from mempool.space using existing client
    let utxos = app_state
        .mempool_client
        .get_address_utxos(&address_str)
        .await?;

    let mut total_balance = 0u64;
    let mut utxo_count = 0;
    let mut utxos_list: HashMap<String, u64> = HashMap::new();

    // For each UTXO, calculate utxo_id and get balance from store
    for utxo in &utxos {
        // Convert txid from hex string to bytes
        let txid_bytes = Vec::from_hex(inverse_hash(&utxo.txid)).map_err(|e| {
            ApiError::ExternalApi(format!("Invalid txid format from mempool: {}", e))
        })?;

        if txid_bytes.len() != 32 {
            return Err(ApiError::ExternalApi(
                "Invalid txid length from mempool".to_string(),
            ));
        }

        // Generate utxo_id using your existing function
        let utxo_id = generate_utxo_id(&txid_bytes, utxo.vout as u64);

        // Get balance from store
        if let Some(balance) = app_state.mhin_store.get_balance(utxo_id) {
            total_balance += balance;
            utxo_count += 1;
            utxos_list.insert(format!("{}:{}", &utxo.txid, utxo.vout), balance);
        }
    }

    let response = AddressBalanceResponse {
        address: address_str,
        total_balance,
        utxo_count,
        utxos: utxos_list,
    };

    Ok(HttpResponse::Ok().json(response))
}
