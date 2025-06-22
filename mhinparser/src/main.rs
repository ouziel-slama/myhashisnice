// main.rs - Updated to use ParserSystemActor with integrated web server
// Bitcoin UTXO Parser main entry point

use actix::prelude::*;
use bitcoin::Network;
use clap::{Arg, ArgMatches, Command};
use ctrlc;
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub mod block;
pub mod blockfetcher;
pub mod blockprocessor;
pub mod config;
pub mod logger;
pub mod parser;
pub mod protocol;
pub mod rpc;
pub mod store;
pub mod web;

use config::MhinConfig;
use parser::{create_parser_system, StartParser, StopParser, GetMhinStore};
use web::{WebServerActor, server::{StartWebServer, StopWebServer}};

/// Parse command line arguments using clap
fn parse_args() -> ArgMatches {
    Command::new("MHIN Parser")
        .version("1.0")
        .author("Ouziel Slama")
        .about("Parses Bitcoin blockchain into UTXO model")
        .arg(
            Arg::new("rpc_url")
                .long("rpc-url")
                .value_name("URL")
                .help("Bitcoin RPC URL (default: http://rpc:rpc@localhost:8332)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("network")
                .long("network")
                .value_name("NETWORK")
                .help("Bitcoin network (bitcoin, testnet, signet, regtest)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("start_height")
                .long("start-height")
                .value_name("HEIGHT")
                .help("Starting block height (default: 232500)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("fetcher_count")
                .long("fetcher-count")
                .value_name("COUNT")
                .help("Number of parallel block fetchers (default: 4)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("buffer_size")
                .long("buffer-size")
                .value_name("SIZE")
                .help("Size of the block buffer per fetcher (default: 25, total = size × fetcher_count)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("web_host")
                .long("web-host")
                .value_name("HOST")
                .help("Web server host (default: 127.0.0.1)")
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("web_port")
                .long("web-port")
                .value_name("PORT")
                .help("Web server port (default: 8080)")
                .value_parser(clap::value_parser!(String)),
        )
        .get_matches()
}

/// Prepare the application configuration from command line args
fn prepare_config(matches: &ArgMatches) -> MhinConfig {
    // Start with default configuration
    let mut config = MhinConfig::default();

    // Apply RPC URL if specified
    if let Some(rpc_url) = matches.get_one::<String>("rpc_url") {
        config.rpc_url = rpc_url.clone();
    }

    // Apply network if specified
    if let Some(network_str) = matches.get_one::<String>("network") {
        config.network = match network_str.to_lowercase().as_str() {
            "bitcoin" => Network::Bitcoin,
            "testnet" => Network::Testnet,
            "signet" => Network::Signet,
            "regtest" => Network::Regtest,
            _ => {
                warn!("Invalid network specified. Using default: Bitcoin");
                Network::Bitcoin
            }
        };
    }

    // Apply start height if specified
    if let Some(start_height_str) = matches.get_one::<String>("start_height") {
        if let Ok(height) = start_height_str.parse::<u64>() {
            config.start_height = height;
        } else {
            warn!(
                "Invalid start height specified. Using default: {}",
                config.start_height
            );
        }
    }

    // Apply fetcher count if specified
    if let Some(fetcher_count_str) = matches.get_one::<String>("fetcher_count") {
        if let Ok(count) = fetcher_count_str.parse::<u64>() {
            config.fetcher_count = count;
        } else {
            warn!(
                "Invalid fetcher count specified. Using default: {}",
                config.fetcher_count
            );
        }
    }

    // Apply buffer size if specified
    if let Some(buffer_size_str) = matches.get_one::<String>("buffer_size") {
        if let Ok(size) = buffer_size_str.parse::<u64>() {
            config.buffer_size = size;
        } else {
            warn!(
                "Invalid buffer size specified. Using default: {}",
                config.buffer_size
            );
        }
    }

    info!("Configuration prepared: {:?}", config);
    config
}

fn main() -> std::io::Result<()> {
    // Initialize logger
    logger::init().expect("Failed to initialize logger");

    // Parse command line arguments
    let matches = parse_args();

    // Prepare configuration using parsed arguments
    let config = prepare_config(&matches);

    // Get web server configuration
    let web_host = matches
        .get_one::<String>("web_host")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    
    let web_port = matches
        .get_one::<String>("web_port")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8080);

    // Create a shared flag for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    // Flag to track if shutdown has already been initiated
    let shutdown_initiated = Arc::new(AtomicBool::new(false));
    let shutdown_initiated_clone = shutdown_initiated.clone();

    // Configure Ctrl+C handler BEFORE starting the system
    ctrlc::set_handler(move || {
        // Only handle Ctrl+C if we haven't already started shutdown
        if !shutdown_initiated_clone.swap(true, Ordering::SeqCst) {
            info!("Ctrl-C received, initiating graceful shutdown...");
            running_clone.store(false, Ordering::SeqCst);
        } else {
            info!("Shutdown already in progress, please wait...");
        }
    })
    .expect("Error setting Ctrl-C handler");

    let system = System::new();

    system.block_on(async {
        // Create the parser system actor
        let parser_system = create_parser_system(config);

        // Start the parser system
        info!("Starting parser system...");
        match parser_system.send(StartParser).await {
            Ok(_) => {
                info!("Parser system started successfully");
            }
            Err(e) => {
                warn!("Failed to start parser system: {:?}", e);
                System::current().stop();
                return;
            }
        }

        // Wait a moment for the parser system to initialize the store
        actix_rt::time::sleep(Duration::from_millis(1000)).await;

        // Get MhinStore from parser system and start web server
        let web_server = match parser_system.send(GetMhinStore).await {
            Ok(Some(mhin_store)) => {
                info!("Got MhinStore reference, starting web server...");
                let web_server = WebServerActor::new(mhin_store).start();
                
                match web_server.send(StartWebServer { 
                    host: web_host.clone(), 
                    port: web_port 
                }).await {
                    Ok(_) => {
                        info!("Web server started on {}:{}", web_host, web_port);
                        Some(web_server)
                    }
                    Err(e) => {
                        warn!("Failed to start web server: {:?}", e);
                        None
                    }
                }
            }
            Ok(None) => {
                warn!("MhinStore not available yet, web server not started");
                None
            }
            Err(e) => {
                warn!("Failed to get MhinStore: {:?}", e);
                None
            }
        };

        info!("System running (press Ctrl-C to stop)");

        // Keep the system running until shutdown is requested
        while running.load(Ordering::SeqCst) {
            actix_rt::time::sleep(Duration::from_millis(100)).await;
        }

        // Gracefully stop the web server first
        if let Some(web_server) = web_server {
            info!("Shutdown requested, stopping web server...");
            let (stop_msg, confirmation_rx) = StopWebServer::with_confirmation();
            match web_server.send(stop_msg).await {
                Ok(_) => {
                    match tokio::time::timeout(Duration::from_secs(5), confirmation_rx).await {
                        Ok(Ok(())) => {
                            info!("Web server stopped successfully");
                        }
                        Ok(Err(_)) => {
                            warn!("Web server confirmation channel closed");
                        }
                        Err(_) => {
                            warn!("Timeout waiting for web server to stop");
                        }
                    }
                }
                Err(e) => {
                    warn!("Error stopping web server: {:?}", e);
                }
            }
        }

        // Then stop the parser system
        info!("Stopping parser system...");
        match parser_system.send(StopParser).await {
            Ok(_) => {
                info!("Parser system stopped successfully");
            }
            Err(e) => {
                warn!("Error stopping parser system: {:?}", e);
            }
        }

        // Give a bit more time for cleanup
        actix_rt::time::sleep(Duration::from_millis(500)).await;

        // Now stop the system
        System::current().stop();
    });

    // Run the system to completion
    system.run()?;
    info!("System shutdown complete");

    Ok(())
}