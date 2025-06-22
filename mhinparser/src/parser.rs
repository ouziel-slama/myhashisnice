// parser.rs - Updated to use per-fetcher quotas and expose MhinStore
use crate::block::BoundedBlockMap;
use crate::blockfetcher::{BlockFetcherActor, StopFetcher};
use crate::blockprocessor::{BlockProcessorActor, StopProcessor};
use crate::config::MhinConfig;
use crate::store::MhinStore;

use actix::prelude::*;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;

// Messages for ParserSystemActor
#[derive(Message)]
#[rtype(result = "()")]
pub struct StartParser;

#[derive(Message)]
#[rtype(result = "()")]
pub struct StopParser;

#[derive(Message)]
#[rtype(result = "()")]
pub struct RestartParser;

// NEW: Message to get MhinStore
#[derive(Message)]
#[rtype(result = "Option<Arc<MhinStore>>")]
pub struct GetMhinStore;

// ParserSystemActor - Actix actor to manage the parsing system
pub struct ParserSystemActor {
    config: MhinConfig,
    processor: Option<Addr<BlockProcessorActor>>,
    fetchers: Vec<Addr<BlockFetcherActor>>,
    is_running: bool,
    mhin_store: Option<Arc<MhinStore>>, // NEW: Store reference
}

impl ParserSystemActor {
    pub fn new(config: MhinConfig) -> Self {
        Self {
            config,
            processor: None,
            fetchers: Vec::new(),
            is_running: false,
            mhin_store: None, // NEW: Initialize as None
        }
    }
}

impl Actor for ParserSystemActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("ParserSystemActor started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("ParserSystemActor stopped");
    }
}

// NEW: Handler for GetMhinStore message
impl Handler<GetMhinStore> for ParserSystemActor {
    type Result = Option<Arc<MhinStore>>;

    fn handle(&mut self, _msg: GetMhinStore, _ctx: &mut Self::Context) -> Self::Result {
        self.mhin_store.clone()
    }
}

// Handler for StartParser message
impl Handler<StartParser> for ParserSystemActor {
    type Result = ();

    fn handle(&mut self, _msg: StartParser, ctx: &mut Self::Context) -> Self::Result {
        if self.is_running {
            info!("Parser system is already running");
            return;
        }

        info!("Starting parser system...");

        // Create the block buffer with per-fetcher quotas
        let blocks = Arc::new(BoundedBlockMap::new(
            self.config.buffer_size as usize,
            self.config.fetcher_count,
        ));

        let total_buffer_size = self.config.buffer_size * self.config.fetcher_count;
        info!(
            "Created block buffer: {} blocks per fetcher, {} total capacity",
            self.config.buffer_size, total_buffer_size
        );

        let parser_address = ctx.address();

        // Create MhinStore (this performs sanity check automatically)
        let mhin_store = Arc::new(MhinStore::new(self.config.clone(), parser_address.clone()));
        
        // NEW: Store the reference
        self.mhin_store = Some(mhin_store.clone());

        // Get the last processed block height from the database
        let last_height = mhin_store.get_last_block_height();
        let start_height = if last_height == 0 {
            self.config.start_height
        } else {
            last_height + 1
        };

        // Start the processor with fetcher_count
        let processor = BlockProcessorActor::new(
            blocks.clone(),
            mhin_store.clone(),
            start_height,
            self.config.fetcher_count,
        )
        .start();
        self.processor = Some(processor);

        // Start all fetchers with unique IDs
        self.fetchers.clear();
        for fetcher_index in 0..self.config.fetcher_count {
            let fetcher = BlockFetcherActor::new(
                self.config.clone(),
                blocks.clone(),
                start_height + fetcher_index,
                self.config.fetcher_count,
                fetcher_index, // Pass fetcher ID
            )
            .start();

            self.fetchers.push(fetcher);
        }

        self.is_running = true;
        info!(
            "Parser system started successfully with {} fetchers",
            self.config.fetcher_count
        );
    }
}

// Handler for StopParser message
impl Handler<StopParser> for ParserSystemActor {
    type Result = ();

    fn handle(&mut self, _msg: StopParser, ctx: &mut Self::Context) -> Self::Result {
        if !self.is_running {
            info!("Parser system is not running");
            return;
        }

        info!("Stopping parser system...");

        // Get the data we need before moving into the async block
        let fetchers = std::mem::take(&mut self.fetchers);
        let processor = self.processor.take();

        let fut = async move {
            // Create channels to collect confirmations from all fetchers
            let mut fetcher_confirmations = Vec::new();

            // Stop all fetchers and collect confirmation receivers
            for (idx, fetcher) in fetchers.iter().enumerate() {
                info!("Stopping fetcher {}", idx);
                let (stop_msg, confirmation_rx) = StopFetcher::with_confirmation();

                if fetcher.send(stop_msg).await.is_ok() {
                    fetcher_confirmations.push(confirmation_rx);
                } else {
                    warn!("Failed to send stop message to fetcher {}", idx);
                }
            }

            // Wait for all fetchers to confirm they've stopped (with timeout)
            info!(
                "Waiting for {} fetchers to confirm shutdown...",
                fetcher_confirmations.len()
            );
            let fetcher_timeout = Duration::from_secs(5);

            for (idx, confirmation_rx) in fetcher_confirmations.into_iter().enumerate() {
                match tokio::time::timeout(fetcher_timeout, confirmation_rx).await {
                    Ok(Ok(())) => {
                        info!("Fetcher {} confirmed shutdown", idx);
                    }
                    Ok(Err(_)) => {
                        warn!("Fetcher {} confirmation channel closed", idx);
                    }
                    Err(_) => {
                        warn!("Timeout waiting for fetcher {} to confirm shutdown", idx);
                    }
                }
            }

            // Stop the processor with confirmation
            if let Some(processor) = &processor {
                info!("Stopping processor");
                let (stop_msg, confirmation_rx) = StopProcessor::with_confirmation();

                if processor.send(stop_msg).await.is_ok() {
                    // Wait for processor confirmation (with timeout)
                    let processor_timeout = Duration::from_secs(5);
                    match tokio::time::timeout(processor_timeout, confirmation_rx).await {
                        Ok(Ok(())) => {
                            info!("Processor confirmed shutdown");
                        }
                        Ok(Err(_)) => {
                            warn!("Processor confirmation channel closed");
                        }
                        Err(_) => {
                            warn!("Timeout waiting for processor to confirm shutdown");
                        }
                    }
                } else {
                    warn!("Failed to send stop message to processor");
                }
            }

            info!("Parser system stopped successfully");
        }
        .into_actor(self)
        .map(|_, actor, _ctx| {
            actor.is_running = false;
            // NEW: Clear the store reference on stop
            actor.mhin_store = None;
        });

        ctx.wait(fut);
    }
}

// Handler for RestartParser message
impl Handler<RestartParser> for ParserSystemActor {
    type Result = ();

    fn handle(&mut self, _msg: RestartParser, ctx: &mut Self::Context) -> Self::Result {
        info!("Restarting parser system...");

        // Get self address to send messages
        let addr = ctx.address();

        let fut = async move {
            // First stop the system
            if let Err(e) = addr.send(StopParser).await {
                warn!("Failed to send stop message during restart: {:?}", e);
                return;
            }

            // Then start it again
            if let Err(e) = addr.send(StartParser).await {
                warn!("Failed to send start message during restart: {:?}", e);
                return;
            }

            info!("Parser system restart completed");
        }
        .into_actor(self)
        .map(|_, _actor, _ctx| {
            // Nothing to do here, the restart is handled by the messages
        });

        ctx.spawn(fut);
    }
}

// Legacy function maintained for compatibility, but now creates and returns the actor address
pub fn create_parser_system(config: MhinConfig) -> Addr<ParserSystemActor> {
    ParserSystemActor::new(config).start()
}