// blockprocessor.rs
use actix::prelude::*;

use crate::block::BoundedBlockMap;
use crate::logger;
use crate::protocol::process_block;
use crate::store::MhinStore;

use log::info;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Message)]
#[rtype(result = "()")]
pub struct ProcessNextBlock;

// Add a message to stop the processor with confirmation
#[derive(Message)]
#[rtype(result = "()")]
pub struct StopProcessor {
    pub confirm_tx: Option<oneshot::Sender<()>>,
}

impl StopProcessor {
    pub fn new() -> Self {
        Self { confirm_tx: None }
    }

    pub fn with_confirmation() -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                confirm_tx: Some(tx),
            },
            rx,
        )
    }
}

// Structure to store block processing history
#[derive(Clone)]
struct BlockProcessingRecord {
    timestamp: Instant,
    blocks_processed: u64,
}

pub struct BlockProcessorActor {
    blocks: Arc<BoundedBlockMap>,
    next_height: u64,
    processing: bool,
    stopping: bool,     // Flag to track stopping state
    fetcher_count: u64, // Track the number of fetchers

    // Performance tracking
    start_time: Instant, // Start time
    blocks_processed: u64,
    blocks_at_last_log: u64, // Track blocks count at last log for rate calculation
    last_performance_log: Instant,

    // History to calculate average speeds
    processing_history: VecDeque<BlockProcessingRecord>, // History of the last 1000 blocks

    mhin_store: Arc<MhinStore>,
}

impl BlockProcessorActor {
    pub fn new(
        blocks: Arc<BoundedBlockMap>,
        mhin_store: Arc<MhinStore>,
        start_height: u64,
        fetcher_count: u64,
    ) -> Self {
        let now = Instant::now();
        BlockProcessorActor {
            blocks,
            next_height: start_height,
            processing: false,
            stopping: false,
            fetcher_count,
            start_time: now, // Initialize start time
            blocks_processed: 0,
            blocks_at_last_log: 0, // Initialize blocks counter for performance tracking
            last_performance_log: now,
            processing_history: VecDeque::with_capacity(1000), // History with capacity of 1000
            mhin_store,
        }
    }

    fn process_next_block(&mut self) {
        if self.processing || self.stopping {
            return;
        }
        if self.blocks.is_empty() {
            return;
        }

        // Set processing flag to prevent concurrent processing
        self.processing = true;

        // Track whether a block was processed
        let mut block_processed = false;

        // Check if we have the block at next_height
        if let Some(block_ref) = self.blocks.get(&self.next_height) {
            // Process the block - panic on failure
            process_block(&block_ref, &self.mhin_store);

            // Drop the reference explicitly to avoid deadlock
            drop(block_ref);

            // Remove the block from the map
            let _ = self.blocks.remove(&self.next_height);

            // Update statistics
            self.blocks_processed += 1;
            self.next_height += 1;

            // Add to processing history
            let now = Instant::now();
            self.processing_history.push_back(BlockProcessingRecord {
                timestamp: now,
                blocks_processed: self.blocks_processed,
            });

            // Keep only the last 1000 records
            if self.processing_history.len() > 1000 {
                self.processing_history.pop_front();
            }

            block_processed = true;
        }

        // Log progress outside the borrow scope if a block was processed
        if block_processed {
            self.log_progress();
        }

        // Reset processing flag
        self.processing = false;
    }

    // Calculate average speed over a given period
    fn calculate_average_speed(&self, period_blocks: usize) -> Option<f64> {
        if self.processing_history.len() < 2 {
            return None;
        }

        let history_len = self.processing_history.len();
        let start_idx = if history_len > period_blocks {
            history_len - period_blocks
        } else {
            0
        };

        if start_idx >= history_len - 1 {
            return None;
        }

        let start_record = &self.processing_history[start_idx];
        let end_record = &self.processing_history[history_len - 1];

        let blocks_diff = end_record.blocks_processed - start_record.blocks_processed;
        let time_diff = end_record
            .timestamp
            .duration_since(start_record.timestamp)
            .as_secs_f64();

        if time_diff > 0.0 {
            Some(blocks_diff as f64 / time_diff)
        } else {
            None
        }
    }

    // Format duration in readable format
    fn format_duration(duration: Duration) -> String {
        let total_secs = duration.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;

        if hours > 0 {
            format!("{}h {:02}m {:02}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {:02}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        }
    }

    fn log_progress(&mut self) {
        const PERF_LOG_INTERVAL_SECS: u64 = 10;

        let now = Instant::now();
        let elapsed = now.duration_since(self.last_performance_log).as_secs();

        if elapsed >= PERF_LOG_INTERVAL_SECS {
            if elapsed > 0 {
                let total_duration = now.duration_since(self.start_time);
                let overall_avg_speed = if total_duration.as_secs() > 0 {
                    self.blocks_processed as f64 / total_duration.as_secs_f64()
                } else {
                    0.0
                };

                let avg_100_blocks = self.calculate_average_speed(100).unwrap_or(0.0);
                let avg_1000_blocks = self.calculate_average_speed(1000).unwrap_or(0.0);

                // Get all stats from the database
                let stats = self.mhin_store.get_all_stats();

                // Helper function to format numbers with thousands separators
                let format_number_with_commas = |num: u64| -> String {
                    let num_str = num.to_string();
                    let mut result = String::new();
                    for (i, c) in num_str.chars().rev().enumerate() {
                        if i > 0 && i % 3 == 0 {
                            result.push(',');
                        }
                        result.push(c);
                    }
                    result.chars().rev().collect()
                };

                // Parse stats once and handle parsing errors gracefully
                let nice_hashes = stats
                    .get("nice_hashes_count")
                    .unwrap_or(&"0".to_string())
                    .parse::<u64>()
                    .unwrap_or(0);
                let unspent = stats
                    .get("unspent_utxo_id_count")
                    .unwrap_or(&"0".to_string())
                    .parse::<u64>()
                    .unwrap_or(0);
                let spent = stats
                    .get("spent_utxo_id_count")
                    .unwrap_or(&"0".to_string())
                    .parse::<u64>()
                    .unwrap_or(0);
                let supply = stats
                    .get("mhin_supply")
                    .unwrap_or(&"0".to_string())
                    .parse::<u64>()
                    .unwrap_or(0);

                // If this is not the first display, clear previous lines
                if self.blocks_at_last_log > 0 && !logger::has_logs_since_last_mark() {
                    print!("\x1B[19A"); // Move up 19 lines
                    print!("\x1B[0J"); // Clear from cursor to end of screen
                }

                println!("=== MHIN Parser Statistics ===");
                println!(
                    "Processed:       {:>10} blocks",
                    format_number_with_commas(self.blocks_processed)
                );
                println!("Current Height:  {:>10}", self.next_height - 1);
                println!(
                    "Runtime:         {:>10}",
                    Self::format_duration(total_duration)
                );
                println!("--------------------------------");
                println!("Overall Avg:     {:>10.1} blocks/sec", overall_avg_speed);
                println!("Last 100 Avg:   {:>10.1} blocks/sec", avg_100_blocks);
                println!("Last 1000 Avg:  {:>10.1} blocks/sec", avg_1000_blocks);
                println!("--------------------------------");

                // Display buffer information with per-fetcher quotas
                let total_capacity = self.blocks.max_size();
                let quota_per_fetcher = self.blocks.get_quota_per_fetcher();
                println!(
                    "Buffer Total:    {:>10} / {} blocks",
                    self.blocks.current_size(),
                    total_capacity
                );
                println!("Quota/Fetcher:   {:>10} blocks", quota_per_fetcher);

                // Show per-fetcher usage for all active fetchers
                let mut fetcher_usage = String::new();
                for i in 0..self.fetcher_count {
                    let count = self.blocks.get_fetcher_count(i);
                    fetcher_usage.push_str(&format!("F{}:{} ", i, count));
                }
                if !fetcher_usage.is_empty() {
                    println!("Fetcher Usage:   {:>10}", fetcher_usage.trim());
                } else {
                    println!("Fetcher Usage:   {:>10}", "No blocks");
                }

                println!("--------------------------------");
                println!(
                    "Nice Hashes:     {:>10}",
                    format_number_with_commas(nice_hashes)
                );

                println!(
                    "MHIN Supply:     {:>10}.{:08}",
                    format_number_with_commas(supply / 100_000_000),
                    supply % 100_000_000
                );

                println!(
                    "Unspent UTXOs:   {:>10}",
                    format_number_with_commas(unspent)
                );
                println!("Spent UTXOs:     {:>10}", format_number_with_commas(spent));

                // Show last nice hash if available
                if let Some(last_hash) = stats.get("last_nice_hash") {
                    if last_hash.len() >= 16 {
                        println!("Last Nice Hash:  {}", last_hash);
                    } else {
                        println!(""); // Empty line to keep the same number of lines
                    }
                } else {
                    println!(""); // Empty line to keep the same number of lines
                }
                println!("===============================");

                // Update the blocks counter for next calculation
                self.blocks_at_last_log = self.blocks_processed;

                // Mark that stats have been displayed
                logger::mark_stats_displayed();
            }
            self.last_performance_log = now;
        }
    }
}

impl Actor for BlockProcessorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("BlockProcessorActor started");

        ctx.run_interval(Duration::from_millis(1), |actor, _ctx| {
            if !actor.stopping {
                actor.process_next_block();
            }
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("BlockProcessorActor stopped");
    }
}

// Updated handler for the StopProcessor message with confirmation
impl Handler<StopProcessor> for BlockProcessorActor {
    type Result = ();

    fn handle(&mut self, msg: StopProcessor, ctx: &mut Self::Context) -> Self::Result {
        info!("BlockProcessorActor received stop signal");
        self.stopping = true;

        // Send confirmation if requested
        if let Some(confirm_tx) = msg.confirm_tx {
            let _ = confirm_tx.send(());
        }

        // If we're not processing, stop the actor immediately
        // Otherwise, it will stop once the current processing is done
        if !self.processing {
            ctx.stop();
        } else {
            // Set a timeout to force stop if processing takes too long
            ctx.run_later(Duration::from_secs(1), |_, ctx| {
                ctx.stop();
            });
        }
    }
}
