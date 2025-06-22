// blockfetcher.rs
use crate::block::BoundedBlockMap;
use crate::config::MhinConfig;
use crate::rpc::fetch_mhin_block;

use actix::prelude::*;
use actix::Actor;

use log::{debug, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

// Add a message to stop the fetcher with confirmation
#[derive(Message)]
#[rtype(result = "()")]
pub struct StopFetcher {
    pub confirm_tx: Option<oneshot::Sender<()>>,
}

impl StopFetcher {
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

pub struct BlockFetcherActor {
    blocks: Arc<BoundedBlockMap>,
    config: MhinConfig,
    next_height: u64,
    step: u64,
    processing: bool,
    stopping: bool,
    consecutive_failures: u64,
    fetcher_id: u64,
}

impl BlockFetcherActor {
    pub fn new(
        config: MhinConfig,
        blocks: Arc<BoundedBlockMap>,
        start_height: u64,
        step: u64,
        fetcher_id: u64,
    ) -> Self {
        BlockFetcherActor {
            blocks,
            config,
            next_height: start_height,
            step,
            processing: false,
            stopping: false,
            consecutive_failures: 0,
            fetcher_id,
        }
    }

    // Updated to check per-fetcher quota instead of global buffer
    fn fetch_next_block(&mut self, ctx: &mut Context<Self>) {
        // Skip fetching if we're stopping or already processing
        if self.stopping || self.processing {
            return;
        }

        // Check per-fetcher quota instead of global buffer
        if !self.blocks.can_fetcher_insert(self.fetcher_id) {
            debug!(
                "Fetcher {} reached quota limit, waiting...",
                self.fetcher_id
            );
            return;
        }

        // Set processing flag to prevent multiple concurrent fetches
        self.processing = true;

        // Get values we need to move into the future
        let next_height = self.next_height;
        let config = self.config.clone();
        let blocks = self.blocks.clone();
        let step = self.step;
        let fetcher_id = self.fetcher_id;

        // Create a future that will fetch the block asynchronously
        let fut = async move {
            match fetch_mhin_block(&config, next_height, fetcher_id).await {
                Some(block) => Ok((block, next_height, step, blocks)),
                None => Err(format!("Failed to fetch block at height {}", next_height)),
            }
        };

        // Convert the future to an ActorFuture and handle the result
        let actor_fut = fut.into_actor(self)
            .map(|result, actor, ctx| {
                match result {
                    Ok((block, height, step, blocks)) => {
                        if blocks.insert(block.height, block) {
                            actor.next_height += step;
                            actor.consecutive_failures = 0; // Reset failure counter on success
                            debug!("Fetcher {} successfully inserted block {} (quota: {}/{})", 
                                   actor.fetcher_id, height, 
                                   blocks.get_fetcher_count(actor.fetcher_id),
                                   blocks.get_quota_per_fetcher());
                        } else {
                            debug!("Fetcher {} failed to insert block at height {} (quota exceeded: {}/{})", 
                                   actor.fetcher_id, height,
                                   blocks.get_fetcher_count(actor.fetcher_id),
                                   blocks.get_quota_per_fetcher());
                        }
                        
                        // Reset processing flag on success
                        actor.processing = false;
                    },
                    Err(_error_msg) => {
                        actor.consecutive_failures += 1;
                        
                        // Calculate backoff delay: 1s, 2s, 3s, ..., max 10s
                        let delay_seconds = std::cmp::min(actor.consecutive_failures, 10);
                        
                        debug!("Fetcher {} block fetch failed, retrying in {}s (attempt {})", 
                              actor.fetcher_id, delay_seconds, actor.consecutive_failures);
                        
                        // Schedule retry and reset processing flag after delay
                        ctx.run_later(Duration::from_secs(delay_seconds), |actor, _ctx| {
                            actor.processing = false;
                            debug!("Fetcher {} retry delay finished, resuming fetch attempts", actor.fetcher_id);
                        });
                    }
                }
            });

        // Skip spawning new futures if we're stopping
        if !self.stopping {
            ctx.spawn(actor_fut);
        } else {
            self.processing = false;
        }
    }
}

impl Actor for BlockFetcherActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(
            "BlockFetcherActor {} started at height {}",
            self.fetcher_id, self.next_height
        );

        ctx.run_interval(Duration::from_millis(3), |actor, ctx| {
            // Only fetch if we're not stopping
            if !actor.stopping {
                actor.fetch_next_block(ctx);
            }
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("BlockFetcherActor {} stopped", self.fetcher_id);
    }
}

// Updated handler for the StopFetcher message with confirmation
impl Handler<StopFetcher> for BlockFetcherActor {
    type Result = ();

    fn handle(&mut self, msg: StopFetcher, ctx: &mut Self::Context) -> Self::Result {
        info!("BlockFetcherActor {} received stop signal", self.fetcher_id);
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
