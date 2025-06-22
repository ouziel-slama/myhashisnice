use bitcoin::Network;

#[derive(Debug, Clone)]
pub struct MhinConfig {
    pub rpc_url: String,
    pub network: Network,
    pub start_height: u64,
    pub fetcher_count: u64,
    pub buffer_size: u64,
    pub min_zero_count: u64,
    pub max_reward: u64,
    pub data_dir: String,
}

impl MhinConfig {
    pub fn new(
        rpc_url: String,
        network: Network,
        start_height: u64,
        fetcher_count: u64,
        buffer_size: u64,
    ) -> Self {
        MhinConfig {
            rpc_url,
            network,
            start_height,
            fetcher_count,
            buffer_size,
            min_zero_count: 6,
            max_reward: 409600000000,
            data_dir: "~/mhindata".to_string(),
        }
    }
}

impl Default for MhinConfig {
    fn default() -> Self {
        MhinConfig {
            rpc_url: "http://rpc:rpc@localhost:8332".to_string(),
            network: Network::Bitcoin,
            start_height: 232500,
            fetcher_count: 4,
            buffer_size: 200, // per fetcher
            min_zero_count: 6,
            max_reward: 409600000000,
            data_dir: "~/mhindata".to_string(),
        }
    }
}
