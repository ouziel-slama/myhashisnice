use crate::web::errors::{ApiError, ApiResult};
use crate::web::models::MempoolUtxo;

use reqwest::Client;

use std::time::Duration;


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