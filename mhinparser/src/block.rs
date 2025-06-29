use crate::config::MhinConfig;
use crate::protocol::{calculate_reward, get_zero_count};

use bitcoin::consensus::encode::serialize_hex;
use bitcoin::opcodes::all::OP_RETURN;
use bitcoin::script::{Instruction, Script};
use bitcoin::Block;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use dashmap::DashMap;
use log::debug;
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};

pub type UtxoId = [u8; 8];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MhinOutput {
    pub utxo_id: UtxoId,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MhinTransaction {
    pub inputs: Vec<UtxoId>,
    pub outputs: Vec<MhinOutput>,
    pub zero_count: u64,
    pub reward: u64,
    pub op_return_distribution: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NiceHash {
    pub txid: String,
    pub reward: u64,
    pub zero_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MhinBlock {
    pub height: u64,
    pub hash: String,
    pub previous_block_hash: String,
    pub transactions: Vec<MhinTransaction>,
    pub max_zero_count: u64,
    pub nice_hashes: Vec<NiceHash>,
    pub fetcher_id: u64,
}

pub struct Pop;
pub struct Add;
pub enum MhinMovementType {
    Pop,
    Add,
}

pub struct MhinMovement {
    pub movement_type: MhinMovementType,
    pub utxo_id: UtxoId,
    pub value: u64,
}

pub type MhinMovements = Vec<MhinMovement>;

pub fn parse_op_return(script_pubkey: &[u8]) -> Vec<u64> {
    let script = Script::from_bytes(script_pubkey);

    // Pattern match on script instructions
    if let [Ok(Instruction::Op(OP_RETURN)), Ok(Instruction::PushBytes(pb))] =
        script.instructions().collect::<Vec<_>>().as_slice()
    {
        let data = pb.as_bytes();

        // Check that data starts with "MHIN" (4 bytes)
        if data.len() < 4 || &data[0..4] != b"MHIN" {
            return Vec::new();
        }

        // CBOR data starts after "MHIN"
        let cbor_data = &data[4..];

        if cbor_data.is_empty() {
            return Vec::new();
        }

        // Decode with ciborium
        let mut cursor = Cursor::new(cbor_data);
        match ciborium::from_reader::<Vec<u64>, _>(&mut cursor) {
            Ok(integers) => integers,
            Err(_) => Vec::new(),
        }
    } else {
        Vec::new()
    }
}

/// Implementation of the From trait to convert from a Bitcoin Block to our MhinBlock
impl From<(&Block, u64, &MhinConfig, u64)> for MhinBlock {
    fn from((block, height, config, fetcher_id): (&Block, u64, &MhinConfig, u64)) -> Self {
        let block_hash = serialize_hex(&block.block_hash());
        let previous_block_hash = serialize_hex(&block.header.prev_blockhash);
        let mut max_zero_count = 0;
        let mut nice_hashes = Vec::new();

        let transactions = block
            .txdata
            .iter()
            // Exclude coinbase transactions
            .filter(|tx| !tx.is_coinbase())
            .map(|tx| {
                let inputs = tx
                    .input
                    .iter()
                    .enumerate()
                    .map(|(_, input)| {
                        let prev_txid = input.previous_output.txid;
                        let prev_vout = input.previous_output.vout;
                        generate_utxo_id(&prev_txid.as_ref(), prev_vout as u64)
                    })
                    .collect();

                let txid_bytes = tx.compute_txid();
                let outputs = tx
                    .output
                    .iter()
                    .enumerate()
                    // Exclude OP_RETURN outputs
                    .filter(|(_, output)| !output.script_pubkey.is_op_return())
                    .map(|(i, output)| {
                        let utxo_id = generate_utxo_id(&txid_bytes.as_ref(), i as u64);
                        MhinOutput {
                            utxo_id,
                            value: output.value.to_sat(),
                        }
                    })
                    .collect();

                let mut op_return_distribution = Vec::new();
                let op_return = tx
                    .output
                    .iter()
                    .find(|output| output.script_pubkey.is_op_return());
                if let Some(op_return) = op_return {
                    op_return_distribution = parse_op_return(op_return.script_pubkey.as_ref());
                }

                let zero_count = get_zero_count(&txid_bytes.as_ref());

                if zero_count >= config.min_zero_count {
                    let txid_string = txid_bytes.to_string();
                    nice_hashes.push(NiceHash {
                        txid: txid_string,
                        reward: 0, // Reward will be calculated later
                        zero_count,
                    });
                }
                if zero_count > max_zero_count {
                    max_zero_count = zero_count;
                }

                MhinTransaction {
                    inputs,
                    outputs,
                    zero_count,
                    reward: 0,
                    op_return_distribution,
                }
            })
            .collect();

        let mut new_block = MhinBlock {
            height,
            hash: block_hash,
            previous_block_hash,
            transactions,
            max_zero_count,
            nice_hashes,
            fetcher_id, // NEW: Set the fetcher ID
        };

        for nice_hash in new_block.nice_hashes.iter_mut() {
            nice_hash.reward =
                calculate_reward(nice_hash.zero_count, new_block.max_zero_count, config);
            debug!(
                "Nice Hash: {} (block: {}, reward: {})",
                nice_hash.txid, height, nice_hash.reward
            );
        }
        for tx in new_block.transactions.iter_mut() {
            tx.reward = calculate_reward(tx.zero_count, new_block.max_zero_count, config);
        }

        new_block
    }
}

impl MhinBlock {
    /// Create a MhinBlock from a Bitcoin library Block
    pub fn from_bitcoin_block(
        block: &Block,
        height: u64,
        config: &MhinConfig,
        fetcher_id: u64,
    ) -> Self {
        Self::from((block, height, config, fetcher_id))
    }
}

/// Calculate a unique UTXO ID by hashing transaction ID and output index
pub fn generate_utxo_id(txid: &[u8], output_index: u64) -> [u8; 8] {
    let mut hasher = Xxh3::new();
    hasher.update(txid);
    hasher.update(&output_index.to_le_bytes());
    let hash = hasher.digest();

    hash.to_le_bytes()
}

/// `BoundedBlockMap` with per-fetcher quotas using DashSets to track fetcher blocks for optimal performance
/// Uses a global DashMap for blocks but DashSets per fetcher to avoid atomic counting operations
pub struct BoundedBlockMap {
    // Global DashMap for all blocks (solves lifetime issues)
    pub blocks: DashMap<u64, MhinBlock>,
    // DashSet per fetcher to track which heights belong to each fetcher (O(1) size lookup)
    fetcher_heights: DashMap<u64, dashmap::DashSet<u64>>,
    count: AtomicUsize,
    max_size: usize,
    quota_per_fetcher: usize,
}

impl BoundedBlockMap {
    /// Create a new BoundedBlockMap with per-fetcher quotas using DashSets for tracking
    pub fn new(quota_per_fetcher: usize, fetcher_count: u64) -> Self {
        let max_size = quota_per_fetcher * fetcher_count as usize;
        let initial_capacity = max_size.next_power_of_two().min(max_size * 2);

        let fetcher_heights = DashMap::with_capacity(fetcher_count as usize);
        for fetcher_id in 0..fetcher_count {
            fetcher_heights.insert(
                fetcher_id,
                dashmap::DashSet::with_capacity(quota_per_fetcher),
            );
        }

        Self {
            blocks: DashMap::with_capacity(initial_capacity),
            fetcher_heights,
            count: AtomicUsize::new(0),
            max_size,
            quota_per_fetcher,
        }
    }

    /// Check if a specific fetcher can insert more blocks - OPTIMIZED: O(1) using DashSet.len()
    pub fn can_fetcher_insert(&self, fetcher_id: u64) -> bool {
        if let Some(heights_set) = self.fetcher_heights.get(&fetcher_id) {
            heights_set.len() < self.quota_per_fetcher
        } else {
            // Fetcher doesn't exist yet, so it can definitely insert
            true
        }
    }

    /// Insert a block if the fetcher hasn't exceeded its quota - OPTIMIZED: uses DashSets for tracking
    pub fn insert(&self, height: u64, block: MhinBlock) -> bool {
        let fetcher_id = block.fetcher_id;

        // Check fetcher quota first (now O(1) using DashSet.len())
        if !self.can_fetcher_insert(fetcher_id) {
            return false;
        }

        // Check global limit as backup
        if self.count.load(Ordering::Acquire) >= self.max_size {
            return false;
        }

        // Get or create the fetcher's DashSet for tracking heights
        let heights_set = match self.fetcher_heights.get(&fetcher_id) {
            Some(set) => set,
            None => {
                // This should not happen with pre-initialization, but handle it gracefully
                panic!("Fetcher {} DashSet not found, creating one", fetcher_id);
            }
        };

        // Each fetcher processes different block heights (step = fetcher_count), no collision possible
        // Insert the block and track the height
        let is_new = self.blocks.insert(height, block).is_none();

        if is_new {
            // Track this height for the fetcher
            heights_set.insert(height);
            //info!("Inserting block at height {} for fetcher {}", height, fetcher_id);
            // Update global counter
            self.count.fetch_add(1, Ordering::Release);
        }

        true
    }

    /// Remove a block and update counters - OPTIMIZED: direct removal from global map
    pub fn remove(&self, height: &u64) -> Option<(u64, MhinBlock)> {
        if let Some((key, block)) = self.blocks.remove(height) {
            let fetcher_id = block.fetcher_id;

            // Remove from fetcher's height tracking set
            if let Some(heights_set) = self.fetcher_heights.get(&fetcher_id) {
                heights_set.remove(height);
            }

            // Update global counter
            self.count.fetch_sub(1, Ordering::Release);

            Some((key, block))
        } else {
            None
        }
    }

    /// Get a reference to a block - OPTIMIZED: direct access to global map (no lifetime issues)
    pub fn get(&self, height: &u64) -> Option<dashmap::mapref::one::Ref<'_, u64, MhinBlock>> {
        self.blocks.get(height)
    }

    /// Get the current total size
    pub fn current_size(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if the global buffer is full
    pub fn is_full(&self) -> bool {
        self.current_size() >= self.max_size
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.current_size() == 0
    }

    /// Get count for a specific fetcher - OPTIMIZED: O(1) using DashSet.len()
    pub fn get_fetcher_count(&self, fetcher_id: u64) -> usize {
        self.fetcher_heights
            .get(&fetcher_id)
            .map(|heights_set| heights_set.len())
            .unwrap_or(0)
    }

    /// Get quota per fetcher
    pub fn get_quota_per_fetcher(&self) -> usize {
        self.quota_per_fetcher
    }

    /// Get max total size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}
