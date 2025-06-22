pub mod balances;
pub mod constants;
pub mod sanity;
pub mod utils;

use crate::block::{MhinBlock, MhinMovementType, MhinMovements, UtxoId};
use crate::config::MhinConfig;
use crate::parser::{ParserSystemActor, RestartParser};
use crate::rpc::make_rpc_call;
use crate::store::balances::UtxoBalances;
use crate::store::constants::{
    ADD_MARKER, BLOCK_MARKER, MAX_FILE_SIZE, NICEHASHES_MARKER, POP_MARKER,
};
use crate::store::utils::{calc_varint_size, write_varint};

use actix::prelude::Addr;
use hex::FromHex;
use log::{error, info};
use rusqlite::{params, Connection, Result as SqlResult};
use serde_json::json;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

pub struct MhinStore {
    pub config: MhinConfig,
    data_dir: PathBuf,
    sqlite_conn: Mutex<Connection>,
    // Replace RwLock<HashMap> with our new UtxoBalances struct
    utxo_balances: UtxoBalances,
    last_block_height: RwLock<u64>,
    current_file_num: Mutex<u32>,
    current_file: Mutex<File>,
    current_file_size: Mutex<u64>,
    parser_system: Addr<ParserSystemActor>,
    last_snapshot_time: RwLock<Instant>,
    last_snapshot_block_height: RwLock<u64>,
}

impl MhinStore {
    pub fn new(config: MhinConfig, parser_system: Addr<ParserSystemActor>) -> Self {
        // Expand tilde in data_dir path
        let data_dir_str = if config.data_dir.starts_with("~/") {
            let home = std::env::var("HOME").expect("HOME environment variable not set");
            config.data_dir.replacen("~/", &format!("{}/", home), 1)
        } else {
            config.data_dir.clone()
        };

        let data_dir = PathBuf::from(data_dir_str);

        // Create data directory if it doesn't exist
        if !data_dir.exists() {
            create_dir_all(&data_dir).expect("Failed to create data directory");
        }

        // Initialize SQLite database
        let sqlite_path = data_dir.join("mhin.db");
        let sqlite_conn = Connection::open(&sqlite_path).expect("Failed to open SQLite database");

        // Create tables if they don't exist
        Self::init_sqlite_tables(&sqlite_conn).expect("Failed to initialize SQLite tables");

        // Initialize or open the data file
        let current_file_num = Self::get_latest_file_num(&data_dir);
        let (current_file, current_file_size) =
            Self::open_or_create_file(&data_dir, current_file_num);

        // Initialize UtxoBalances with pre-allocated capacity to avoid rehashing
        let utxo_balances = UtxoBalances::new(100_000_000, &data_dir); // Pre-allocate for 100M UTXOs
        let last_block_height = RwLock::new(0);

        let store = MhinStore {
            config,
            data_dir,
            sqlite_conn: Mutex::new(sqlite_conn),
            utxo_balances,
            last_block_height,
            current_file_num: Mutex::new(current_file_num),
            current_file: Mutex::new(current_file),
            current_file_size: Mutex::new(current_file_size),
            parser_system,
            last_snapshot_time: RwLock::new(Instant::now()),
            last_snapshot_block_height: RwLock::new(0),
        };

        // Load all movements from binary files to reconstruct UTXO state
        store.utxo_balances.load_all_movements_from_files(
            &store.sqlite_conn,
            &store.data_dir,
            &store.last_block_height,
        );

        // Perform sanity check to ensure data consistency
        store.perform_sanity_check();

        store
    }

    /// Initialize SQLite tables if they don't exist
    fn init_sqlite_tables(conn: &Connection) -> SqlResult<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                height INTEGER PRIMARY KEY,
                file_num INTEGER NOT NULL,
                file_pos INTEGER NOT NULL,
                movements_length INTEGER NOT NULL,
                block_hash TEXT NOT NULL DEFAULT ''
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS nicehashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_height INTEGER NOT NULL,
                tx_id TEXT NOT NULL,
                zero_count INTEGER NOT NULL,
                reward INTEGER NOT NULL,
                FOREIGN KEY (block_height) REFERENCES blocks(height) ON DELETE CASCADE,
                UNIQUE(block_height, tx_id)
            )",
            [],
        )?;

        // Create stats table for cached counters
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        Ok(())
    }

    /// Get a stat value from the stats table
    fn get_stat(&self, conn: &Connection, key: &str) -> SqlResult<Option<String>> {
        match conn.query_row(
            "SELECT value FROM stats WHERE key = ?1",
            params![key],
            |row| row.get::<_, String>(0),
        ) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Set a stat value in the stats table
    fn set_stat(&self, conn: &Connection, key: &str, value: &str) -> SqlResult<()> {
        conn.execute(
            "INSERT OR REPLACE INTO stats (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    /// Increment a numeric stat by a given amount
    fn increment_stat(&self, conn: &Connection, key: &str, increment: i64) -> SqlResult<()> {
        let current_value = self
            .get_stat(conn, key)?
            .unwrap_or_else(|| "0".to_string())
            .parse::<i64>()
            .unwrap_or(0);

        let new_value = current_value + increment;
        self.set_stat(conn, key, &new_value.to_string())
    }

    /// Get or create a data file for storing MHIN movements
    fn open_or_create_file(data_dir: &Path, file_num: u32) -> (File, u64) {
        let file_path = data_dir.join(format!("mhin_{:05}.bin", file_num));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .expect("Failed to open or create data file");

        let file_size = file.metadata().expect("Failed to get file metadata").len();

        (file, file_size)
    }

    /// Get the latest file number by scanning the data directory
    fn get_latest_file_num(data_dir: &Path) -> u32 {
        let pattern = data_dir.join("mhin_*.bin");
        let pattern_str = pattern.to_string_lossy();

        let entries = match glob::glob(&pattern_str) {
            Ok(entries) => entries,
            Err(_) => return 0,
        };

        let mut max_file_num = 0;

        for entry in entries {
            if let Ok(path) = entry {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        if let Some(captures) = regex::Regex::new(r"mhin_(\d{5})\.bin")
                            .unwrap()
                            .captures(file_name_str)
                        {
                            if let Some(num_str) = captures.get(1) {
                                if let Ok(num) = num_str.as_str().parse::<u32>() {
                                    if num > max_file_num {
                                        max_file_num = num;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        max_file_num
    }

    /// Get the balance of a UTXO - concurrent read operation using RwLock
    pub fn get_balance(&self, utxo_id: UtxoId) -> Option<u64> {
        self.utxo_balances.get_balance(utxo_id)
    }

    /// Check if it's time to create a UTXO snapshot (every 30 minutes AND at least 100 blocks processed)
    fn should_create_snapshot(&self, current_block_height: u64) -> bool {
        const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes
        //const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5 * 60);
        const MIN_BLOCKS_BETWEEN_SNAPSHOTS: u64 = 100; // Minimum 100 blocks

        let last_snapshot_time = self.last_snapshot_time.read().unwrap();
        let last_snapshot_block = self.last_snapshot_block_height.read().unwrap();

        // Check both conditions: time elapsed AND minimum blocks processed
        let time_elapsed = last_snapshot_time.elapsed() >= SNAPSHOT_INTERVAL;
        let blocks_processed =
            current_block_height >= *last_snapshot_block + MIN_BLOCKS_BETWEEN_SNAPSHOTS;

        time_elapsed && blocks_processed
    }

    /// Create a UTXO snapshot if enough time has passed AND enough blocks processed
    /// This function is now SYNCHRONOUS and blocks until the snapshot is completed
    fn maybe_create_snapshot(&self, block_height: u64) {
        // Check if already in RocksDB mode first
        let state = self.utxo_balances.state.read().unwrap();
        if let crate::store::balances::UtxoBalancesState::RocksDB(_) = &*state {
            // Already in persistent mode, no need for snapshots
            return;
        }
        drop(state);

        if self.should_create_snapshot(block_height) {
            info!("Creating UTXO snapshot at block {} (criteria: 30min elapsed + 100+ blocks processed)", block_height);

            // Update timestamps/block height BEFORE starting the snapshot
            // to prevent concurrent snapshots
            {
                let mut last_snapshot_time = self.last_snapshot_time.write().unwrap();
                let mut last_snapshot_block = self.last_snapshot_block_height.write().unwrap();
                *last_snapshot_time = Instant::now();
                *last_snapshot_block = block_height;
            }

            // Check if we're at the blockchain tip
            let rpc_url = self.config.rpc_url.clone();
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    make_rpc_call(&rpc_url, "getblockcount", json!([])).await
                })
            });

            let blockchain_tip = handle.join().unwrap();
            
            if let Some(tip_value) = blockchain_tip {
                if let Some(tip_height) = tip_value.as_u64() {
                    if block_height == tip_height {
                        info!("At blockchain tip (block {}), switching to persistent mode", block_height);
                        if let Err(e) = self.utxo_balances.switch_to_persistent_mode() {
                            panic!("Failed to switch to persistent mode: {}", e);
                        }
                        info!("Successfully switched to persistent mode at block {}", block_height);
                        return;
                    }
                }
            }

            // If not at tip or RPC failed, create regular snapshot
            self.utxo_balances
                .dump_balances(&self.data_dir, block_height)
                .expect("Failed to create UTXO snapshot - this should never happen!");

            info!(
                "UTXO snapshot completed successfully at block {}",
                block_height
            );
        }
    }

    /// Add a block and its movements to all databases
    pub fn add_block(&self, block: &MhinBlock, movements: &MhinMovements) {
        let previous_block_hash = self.get_last_block_hash();
        if previous_block_hash != "".to_string() && block.previous_block_hash != previous_block_hash
        {
            error!(
                "Reorg detected: block {} has an invalid previous block hash: expected {}, got {}. Restarting...", 
                block.height, previous_block_hash, block.previous_block_hash
            );
            self.parser_system.do_send(RestartParser);
            return;
        }

        // Start transaction in SQLite with error handling
        let mut sqlite_conn = self
            .sqlite_conn
            .lock()
            .expect("Failed to acquire SQLite connection lock");

        let tx = sqlite_conn
            .transaction()
            .expect("Failed to begin SQLite transaction");

        // Write block and movements to binary file
        let (file_num, file_pos, movements_length) = self
            .write_block_to_file(block, movements)
            .expect("Failed to write block to file");

        // Insert block information into SQLite
        tx.execute(
            "INSERT INTO blocks (height, file_num, file_pos, movements_length, block_hash) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                block.height as i64,
                file_num as i64,
                file_pos as i64,
                movements_length as i64,
                &block.hash
            ],
        )
        .expect("Failed to insert block into SQLite");

        // Track nice hash statistics
        let mut nicest_hash_in_block = "".to_string();
        let mut supply_change = 0i64;

        let mut current_nicest_zero_count = self
            .get_stat(&tx, "nicest_hash_zero_count")
            .unwrap_or(Some("0".to_string()))
            .unwrap_or_else(|| "0".to_string())
            .parse::<u64>()
            .unwrap_or(0);

        // Insert nice hashes into SQLite
        for nice_hash in &block.nice_hashes {
            supply_change += nice_hash.reward as i64;
            tx.execute(
                "INSERT INTO nicehashes (block_height, tx_id, zero_count, reward) 
                    VALUES (?1, ?2, ?3, ?4)",
                params![
                    block.height as i64,
                    nice_hash.txid,
                    nice_hash.zero_count as i64,
                    nice_hash.reward as i64
                ],
            )
            .expect("Failed to insert nice hash into SQLite");

            // Track the nicest hash in this block
            if nice_hash.zero_count > current_nicest_zero_count {
                current_nicest_zero_count = nice_hash.zero_count;
                nicest_hash_in_block = nice_hash.txid.clone();
            }
        }

        // Update stats for nice hashes
        if !block.nice_hashes.is_empty() {
            // Increment nice hashes count
            self.increment_stat(&tx, "nice_hashes_count", block.nice_hashes.len() as i64)
                .expect("Failed to update nice_hashes_count stat");

            // Update last nice hash
            self.set_stat(
                &tx,
                "last_nice_hash",
                &block.nice_hashes[block.nice_hashes.len() - 1].txid,
            )
            .expect("Failed to update last_nice_hash stat");

            // Set first nice hash if not already set
            if self
                .get_stat(&tx, "first_nice_hash")
                .unwrap_or(None)
                .is_none()
            {
                self.set_stat(&tx, "first_nice_hash", &block.nice_hashes[0].txid)
                    .expect("Failed to set first_nice_hash stat");
            }

            // Update nicest hash if this block has a nicer one
            if nicest_hash_in_block != "".to_string() {
                self.set_stat(&tx, "nicest_hash", &nicest_hash_in_block)
                    .expect("Failed to update nicest_hash stat");
                self.set_stat(
                    &tx,
                    "nicest_hash_zero_count",
                    &block.max_zero_count.to_string(),
                )
                .expect("Failed to update nicest_hash_zero_count stat");
            }
        }

        // Track movement statistics
        let mut add_count = 0i64;
        let mut pop_count = 0i64;

        // Update balances in memory using UtxoBalances - acquire write lock once for all movements
        {
            let mut balances = self.utxo_balances.write_lock();
            for movement in movements {
                match movement.movement_type {
                    MhinMovementType::Add => {
                        balances.insert(movement.utxo_id, movement.value);
                        add_count += 1;
                    }
                    MhinMovementType::Pop => {
                        balances.remove(&movement.utxo_id);
                        pop_count += 1;
                    }
                }
            }
        } // Write lock is automatically dropped here

        // Update movement statistics
        if add_count > 0 {
            self.increment_stat(&tx, "unspent_utxo_id_count", add_count)
                .expect("Failed to update unspent_utxo_id_count stat");
        }

        if pop_count > 0 {
            self.increment_stat(&tx, "spent_utxo_id_count", pop_count)
                .expect("Failed to update spent_utxo_id_count stat");
            self.increment_stat(&tx, "unspent_utxo_id_count", -pop_count)
                .expect("Failed to decrement unspent_utxo_id_count stat");
        }

        // Update supply
        if supply_change != 0 {
            self.increment_stat(&tx, "mhin_supply", supply_change)
                .expect("Failed to update mhin_supply stat");
        }

        // Store the current block height as the last processed block
        {
            let mut last_height = self.last_block_height.write().unwrap();
            *last_height = block.height;
        }

        // Commit SQLite transaction
        tx.commit().expect("Failed to commit SQLite transaction");

        // Check if we need to create a snapshot (after successful block processing)
        self.maybe_create_snapshot(block.height);
    }

    /// Get the height of the last processed block from memory
    pub fn get_last_parsed_block_height(&self) -> Option<u64> {
        let last_height = self.last_block_height.read().unwrap();
        if *last_height > 0 {
            Some(*last_height)
        } else {
            None
        }
    }

    /// Write block and movements to binary file
    fn write_block_to_file(
        &self,
        block: &MhinBlock,
        movements: &MhinMovements,
    ) -> io::Result<(u32, u64, u64)> {
        let mut file_guard = self.current_file.lock().unwrap();
        let mut file_size_guard = self.current_file_size.lock().unwrap();
        let mut file_num_guard = self.current_file_num.lock().unwrap();

        // Calculate the size needed for this block
        let movements_size: u64 = movements.iter().map(|_| 1 + 8 + 8).sum::<u64>(); // 1 for marker, 8 for utxo_id, 8 for amount
                                                                                    // Calculate nice hashes size: 1 byte marker + varint for count + count * 32 bytes for hashes
        let nice_hashes_size = 1
            + calc_varint_size(block.nice_hashes.len() as u64)
            + (block.nice_hashes.len() as u64 * 32);
        let block_header_size = 1 + 3 + calc_varint_size(movements_size); // 1 for marker, 3 for height, varint for movements_length
        let total_size = block_header_size + movements_size + nice_hashes_size;

        // Check if we need a new file
        if *file_size_guard + total_size > MAX_FILE_SIZE {
            // Create a new file
            *file_num_guard += 1;
            let (new_file, new_size) = Self::open_or_create_file(&self.data_dir, *file_num_guard);
            *file_guard = new_file;
            *file_size_guard = new_size;
        }

        // Get the current position
        let file_pos = file_guard.seek(SeekFrom::End(0))?;

        // Write block marker
        file_guard.write_all(&[BLOCK_MARKER])?;

        // Write height (3 bytes, little endian)
        let height_bytes = (block.height as u32).to_le_bytes();
        file_guard.write_all(&height_bytes[0..3])?;

        // Write movements length as varint
        write_varint(&mut *file_guard, movements_size)?;

        // Write all movements
        for movement in movements {
            match movement.movement_type {
                MhinMovementType::Add => {
                    file_guard.write_all(&[ADD_MARKER])?;
                }
                MhinMovementType::Pop => {
                    file_guard.write_all(&[POP_MARKER])?;
                }
            }

            // Write utxo_id
            file_guard.write_all(&movement.utxo_id)?;

            // Write amount (8 bytes, little endian)
            file_guard.write_all(&movement.value.to_le_bytes())?;
        }

        // Write nice hashes marker
        file_guard.write_all(&[NICEHASHES_MARKER])?;

        // Write number of nice hashes as varint
        write_varint(&mut *file_guard, block.nice_hashes.len() as u64)?;

        // Write all nice hashes (each hash is 32 bytes)
        for nice_hash in &block.nice_hashes {
            // Convert hex string to bytes (32 bytes)
            let hash_bytes = Vec::from_hex(nice_hash.txid.clone()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid hex hash: {}", e),
                )
            })?;
            if hash_bytes.len() != 32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Hash must be 32 bytes",
                ));
            }
            file_guard.write_all(&hash_bytes)?;
        }

        // Update the file size
        *file_size_guard += total_size;

        Ok((*file_num_guard, file_pos, movements_size))
    }

    /// Get the height of the last processed block
    pub fn get_last_block_height(&self) -> u64 {
        let sqlite_conn = self.sqlite_conn.lock().unwrap();

        // Query the maximum height from the blocks table
        sqlite_conn
            .query_row("SELECT MAX(height) FROM blocks", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .expect("Failed to query last block height")
            .map(|height| height as u64)
            .unwrap_or(0)
    }

    pub fn get_last_block_hash(&self) -> String {
        let sqlite_conn = self.sqlite_conn.lock().unwrap();

        // Query the maximum height from the blocks table
        match sqlite_conn.query_row(
            "SELECT block_hash FROM blocks ORDER BY height DESC LIMIT 1",
            [],
            |row| row.get::<_, Option<String>>(0),
        ) {
            Ok(Some(hash)) => hash,
            Ok(None) | Err(_) => "".to_string(),
        }
    }

    /// Get all stats from the stats table
    pub fn get_all_stats(&self) -> std::collections::HashMap<String, String> {
        let sqlite_conn = self.sqlite_conn.lock().unwrap();
        let mut stats = std::collections::HashMap::new();

        // Query all stats from the database
        let mut stmt = sqlite_conn
            .prepare("SELECT key, value FROM stats ORDER BY key")
            .expect("Failed to prepare stats query");

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("Failed to execute stats query");

        for row in rows {
            let (key, value) = row.expect("Failed to read stat row");
            stats.insert(key, value);
        }

        // Add block_height directly using the already locked connection
        // to avoid deadlock with get_last_block_height()
        let block_height = sqlite_conn
            .query_row("SELECT MAX(height) FROM blocks", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .expect("Failed to query last block height")
            .map(|height| height as u64)
            .unwrap_or(0);

        stats.insert(
            "block_height".to_string(),
            block_height.to_string(),
        );

        stats
    }

    /// Get paginated nice hashes for web API
    pub fn get_nicehashes_paginated(&self, limit: u32, offset: u32) -> Result<(Vec<(u64, String, u64, u64)>, u64), String> {
        let sqlite_conn = self.sqlite_conn.lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;

        // Get total count
        let total: u64 = sqlite_conn
            .query_row("SELECT COUNT(*) FROM nicehashes", [], |row| {
                Ok(row.get::<_, i64>(0)? as u64)
            })
            .map_err(|e| format!("Failed to count nicehashes: {}", e))?;

        // Get paginated results
        let mut stmt = sqlite_conn
            .prepare(
                "SELECT block_height, tx_id, zero_count, reward 
                 FROM nicehashes 
                 ORDER BY block_height DESC, zero_count DESC 
                 LIMIT ?1 OFFSET ?2",
            )
            .map_err(|e| format!("Failed to prepare query: {}", e))?;

        let nicehash_iter = stmt
            .query_map(params![limit as i64, offset as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,  // block_height
                    row.get::<_, String>(1)?,      // tx_id
                    row.get::<_, i64>(2)? as u64,  // zero_count
                    row.get::<_, i64>(3)? as u64,  // reward
                ))
            })
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        let mut nicehashes = Vec::new();
        for nicehash in nicehash_iter {
            nicehashes.push(
                nicehash.map_err(|e| format!("Failed to read row: {}", e))?,
            );
        }

        Ok((nicehashes, total))
    }
}