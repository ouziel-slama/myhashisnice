use crate::block::MhinBlock;
use crate::config::MhinConfig;

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use hex::FromHex;
use log::debug;
use reqwest::Client;
use serde_json::{json, Value};

/// Helper function to make a Bitcoin RPC call asynchronously
pub async fn make_rpc_call(url: &str, method: &str, params: Value) -> Option<Value> {
    let client = Client::new();
    let request_body = json!({
        "jsonrpc": "1.0",
        "id": "mhin",
        "method": method,
        "params": params
    });

    // Make the HTTP POST request to the Bitcoin RPC endpoint asynchronously
    let response = client.post(url).json(&request_body).send().await.ok()?;

    // Check if the request was successful
    if !response.status().is_success() {
        debug!("RPC call failed with status: {}", response.status());
        return None;
    }

    // Parse the JSON response asynchronously
    let response_body: Value = response.json().await.ok()?;

    // Check for RPC errors
    if response_body.get("error").is_some() && !response_body.get("error")?.is_null() {
        debug!("RPC call returned error: {:?}", response_body.get("error"));
        return None;
    }

    // Return the result field
    response_body.get("result").cloned()
}

/// Fetch a Bitcoin block from RPC asynchronously
pub async fn fetch_bitcoin_block(rpc_url: &str, block_height: u64) -> Option<(Block, u64)> {
    // 1. Fetch the block hash with RPC using getblockhash method
    let block_hash_value = make_rpc_call(rpc_url, "getblockhash", json!([block_height])).await?;

    let block_hash = block_hash_value.as_str()?.to_string();

    // 2. Fetch the block in raw hexadecimal format (verbosity=0)
    let block_hex = make_rpc_call(
        rpc_url,
        "getblock",
        json!([block_hash, 0]), // verbosity level 0 for hex data
    )
    .await?;

    let block_hex_str = block_hex.as_str()?;

    // 3. Parse the hex data into a Bitcoin block
    let block_bytes = Vec::from_hex(block_hex_str).ok()?;
    let block: Block = deserialize(&block_bytes).ok()?;

    Some((block, block_height))
}

/// Fetch a block and convert it to MhinBlock asynchronously
/// NEW: Added fetcher_id parameter
pub async fn fetch_mhin_block(
    config: &MhinConfig,
    block_height: u64,
    fetcher_id: u64,
) -> Option<MhinBlock> {
    // 1. Fetch the Bitcoin block
    let (block, height) = fetch_bitcoin_block(&config.rpc_url, block_height).await?;

    // 2. Convert to MhinBlock with fetcher_id
    Some(MhinBlock::from_bitcoin_block(
        &block, height, config, fetcher_id,
    ))
}
