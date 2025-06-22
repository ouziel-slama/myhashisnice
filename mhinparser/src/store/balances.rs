use crate::block::UtxoId;
use crate::store::constants::{ADD_MARKER, POP_MARKER};
use crate::store::utils::read_varint;

use log::{info, debug, warn};
use rocksdb::{Options, WriteBatch, DB};
use rusqlite::Connection;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub enum UtxoBalancesState {
    HashMap(HashMap<UtxoId, u64>),
    RocksDB(Arc<DB>),
}

pub struct UtxoBalances {
    pub state: RwLock<UtxoBalancesState>,
    data_dir: PathBuf,
}

pub enum UtxoBalancesWriteLock<'a> {
    HashMap(std::sync::RwLockWriteGuard<'a, UtxoBalancesState>),
    RocksDB {
        _guard: std::sync::RwLockReadGuard<'a, UtxoBalancesState>,
        db: Arc<DB>,
        batch: WriteBatch,
    },
}

impl<'a> UtxoBalancesWriteLock<'a> {
    pub fn insert(&mut self, utxo_id: UtxoId, value: u64) {
        match self {
            UtxoBalancesWriteLock::HashMap(guard) => {
                if let UtxoBalancesState::HashMap(ref mut map) = &mut **guard {
                    map.insert(utxo_id, value);
                }
            }
            UtxoBalancesWriteLock::RocksDB { batch, .. } => {
                batch.put(&utxo_id, &value.to_le_bytes());
            }
        }
    }

    pub fn remove(&mut self, utxo_id: &UtxoId) {
        match self {
            UtxoBalancesWriteLock::HashMap(guard) => {
                if let UtxoBalancesState::HashMap(ref mut map) = &mut **guard {
                    map.remove(utxo_id);
                }
            }
            UtxoBalancesWriteLock::RocksDB { batch, .. } => {
                batch.delete(utxo_id);
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            UtxoBalancesWriteLock::HashMap(guard) => {
                if let UtxoBalancesState::HashMap(ref map) = &**guard {
                    map.len()
                } else {
                    0
                }
            }
            UtxoBalancesWriteLock::RocksDB { db, .. } => {
                // Expensive operation, avoid if possible
                let mut count = 0;
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for _ in iter {
                    count += 1;
                }
                count
            }
        }
    }
}

impl<'a> Drop for UtxoBalancesWriteLock<'a> {
    fn drop(&mut self) {
        if let UtxoBalancesWriteLock::RocksDB { db, batch, .. } = self {
            let batch = std::mem::take(batch);
            db.write(batch).unwrap();
        }
    }
}

impl UtxoBalances {
    /// Open RocksDB with optimized settings
    fn open_rocksdb(db_path: &PathBuf, for_bulk_load: bool) -> io::Result<Arc<DB>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        
        if for_bulk_load {
            // Bulk load optimizations
            opts.set_write_buffer_size(512 * 1024 * 1024); // 512MB for bulk load
            opts.set_max_write_buffer_number(8);
            opts.set_target_file_size_base(512 * 1024 * 1024);
            opts.set_max_bytes_for_level_base(2048 * 1024 * 1024);
            opts.set_max_open_files(-1); // No limit
            opts.set_use_fsync(false);
            opts.set_bytes_per_sync(0); // Disable during bulk load
            opts.set_disable_auto_compactions(true); // Disable during bulk load
        } else {
            // Normal operation settings
            opts.set_write_buffer_size(256 * 1024 * 1024); // 256MB
            opts.set_max_write_buffer_number(4);
            opts.set_target_file_size_base(256 * 1024 * 1024);
            opts.set_max_bytes_for_level_base(1024 * 1024 * 1024);
            opts.set_max_open_files(-1);
            opts.set_use_fsync(true);
            opts.set_disable_auto_compactions(false);
        }

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        if for_bulk_load {
            block_opts.set_block_size(64 * 1024); // Larger blocks for bulk load
        } else {
            block_opts.set_block_size(16 * 1024); // Standard size
        }
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);

        let db = DB::open(&opts, db_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Arc::new(db))
    }

    pub fn new(capacity: usize, data_dir: &PathBuf) -> Self {
        let db_path = data_dir.join("utxo_balances_rockdb");
        
        // Check if RocksDB already exists
        if db_path.exists() && std::fs::metadata(&db_path).map(|m| m.is_dir()).unwrap_or(false) {
            info!("Existing RocksDB found, initializing in persistent mode");
            
            match Self::open_rocksdb(&db_path, false) {
                Ok(db) => {
                    info!("Successfully opened existing RocksDB");
                    return Self {
                        state: RwLock::new(UtxoBalancesState::RocksDB(db)),
                        data_dir: data_dir.clone(),
                    };
                }
                Err(e) => {
                    warn!("Failed to open existing RocksDB ({}), falling back to HashMap mode", e);
                }
            }
        } else {
            info!("No existing RocksDB found, initializing in HashMap mode");
        }

        // Fallback to HashMap mode
        Self {
            state: RwLock::new(UtxoBalancesState::HashMap(HashMap::with_capacity(capacity))),
            data_dir: data_dir.clone(),
        }
    }

    pub fn get_balance(&self, utxo_id: UtxoId) -> Option<u64> {
        let state = self.state.read().unwrap();
        match &*state {
            UtxoBalancesState::HashMap(map) => map.get(&utxo_id).copied(),
            UtxoBalancesState::RocksDB(db) => db
                .get(&utxo_id)
                .unwrap()
                .map(|bytes| u64::from_le_bytes(bytes.as_slice().try_into().unwrap())),
        }
    }

    pub fn insert(&self, utxo_id: UtxoId, value: u64) {
        let mut lock = self.write_lock();
        lock.insert(utxo_id, value);
    }

    pub fn remove(&self, utxo_id: &UtxoId) {
        let mut lock = self.write_lock();
        lock.remove(utxo_id);
    }

    pub fn len(&self) -> usize {
        let state = self.state.read().unwrap();
        match &*state {
            UtxoBalancesState::HashMap(map) => map.len(),
            UtxoBalancesState::RocksDB(db) => {
                // Cache this value if called frequently
                let mut count = 0;
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for _ in iter {
                    count += 1;
                }
                count
            }
        }
    }

    pub fn write_lock(&self) -> UtxoBalancesWriteLock {
        let state = self.state.read().unwrap();
        match &*state {
            UtxoBalancesState::HashMap(_) => {
                drop(state);
                UtxoBalancesWriteLock::HashMap(self.state.write().unwrap())
            }
            UtxoBalancesState::RocksDB(db) => UtxoBalancesWriteLock::RocksDB {
                db: db.clone(),
                _guard: state,
                batch: WriteBatch::default(),
            },
        }
    }

    pub fn switch_to_persistent_mode(&self) -> io::Result<()> {
        let mut state_guard = self.state.write().unwrap();

        let hashmap = match &*state_guard {
            UtxoBalancesState::HashMap(map) => map.clone(), // Clone for migration
            UtxoBalancesState::RocksDB(_) => {
                debug!("Already in RocksDB mode");
                return Ok(());
            }
        };

        info!("Starting migration from HashMap to RocksDB...");

        let db_path = self.data_dir.join("utxo_balances_rockdb");
        std::fs::create_dir_all(&db_path)?;

        let db = Self::open_rocksdb(&db_path, true)?; // Use bulk load settings

        // Bulk load with large batches
        info!(
            "Migrating {} UTXOs from HashMap to RocksDB...",
            hashmap.len()
        );
        let start_time = std::time::Instant::now();

        let mut batch = WriteBatch::default();
        let mut count = 0;
        const BATCH_SIZE: usize = 500_000; // Larger batches for bulk load

        for (utxo_id, value) in hashmap.iter() {
            batch.put(utxo_id, &value.to_le_bytes());
            count += 1;

            if count % BATCH_SIZE == 0 {
                db.write(batch)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                batch = WriteBatch::default();

                if count % (BATCH_SIZE * 4) == 0 {
                    info!("Migration progress: {} / {} UTXOs", count, hashmap.len());
                }
            }
        }

        // Final batch
        if !batch.is_empty() {
            db.write(batch)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // Re-enable auto compactions
        db.set_options(&[("disable_auto_compactions", "false")])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Switch to RocksDB state
        *state_guard = UtxoBalancesState::RocksDB(db);

        let duration = start_time.elapsed();
        info!(
            "Migration completed in {:.2}s: {} UTXOs migrated to RocksDB",
            duration.as_secs_f64(),
            count
        );

        Ok(())
    }

    /// Clean up old snapshot files after migration to RocksDB
    pub fn cleanup_snapshots_after_migration(&self, data_dir: &PathBuf) {
        let state = self.state.read().unwrap();
        if let UtxoBalancesState::RocksDB(_) = &*state {
            info!("Cleaning up old snapshot files after migration to RocksDB...");
            let pattern = data_dir.join("utxo_snapshot_*.bin");
            let pattern_str = pattern.to_string_lossy();

            let entries = match glob::glob(&pattern_str) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Failed to glob snapshot files for cleanup: {}", e);
                    return;
                }
            };

            let mut deleted_count = 0;
            for entry in entries {
                if let Ok(path) = entry {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {
                            info!("Deleted old snapshot: {:?}", path.file_name());
                            deleted_count += 1;
                        }
                        Err(e) => {
                            warn!("Failed to delete old snapshot {:?}: {}", path, e);
                        }
                    }
                }
            }
            
            if deleted_count > 0 {
                info!("Successfully cleaned up {} old snapshot files", deleted_count);
            }
        }
    }

    pub fn dump_balances(&self, data_dir: &PathBuf, block_height: u64) -> io::Result<()> {
        let state = self.state.read().unwrap();
        match &*state {
            UtxoBalancesState::HashMap(map) => {
                self.dump_hashmap_snapshot(map, data_dir, block_height)
            }
            UtxoBalancesState::RocksDB(_) => {
                info!(
                    "RocksDB snapshot not needed at block {} - data is already persistent",
                    block_height
                );
                Ok(())
            }
        }
    }

    /// Clean up old snapshot files, keeping only the latest one
    fn cleanup_old_snapshots(&self, data_dir: &PathBuf, current_block_height: u64) {
        let pattern = data_dir.join("utxo_snapshot_*.bin");
        let pattern_str = pattern.to_string_lossy();

        let entries = match glob::glob(&pattern_str) {
            Ok(entries) => entries,
            Err(e) => {
                info!("Failed to glob snapshot files for cleanup: {}", e);
                return;
            }
        };

        let mut old_snapshots = Vec::new();

        for entry in entries {
            if let Ok(path) = entry {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        if let Some(captures) = regex::Regex::new(r"utxo_snapshot_(\d+)\.bin")
                            .unwrap()
                            .captures(file_name_str)
                        {
                            if let Some(height_str) = captures.get(1) {
                                if let Ok(height) = height_str.as_str().parse::<u64>() {
                                    // Only delete snapshots older than the current one
                                    if height < current_block_height {
                                        old_snapshots.push((height, path));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if old_snapshots.is_empty() {
            return;
        }

        // Sort by height to delete oldest first
        old_snapshots.sort_by_key(|(height, _)| *height);

        info!("Cleaning up {} old snapshot files...", old_snapshots.len());
        let mut deleted_count = 0;

        for (height, path) in old_snapshots {
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    info!("Deleted old snapshot: utxo_snapshot_{}.bin", height);
                    deleted_count += 1;
                }
                Err(e) => {
                    info!("Failed to delete old snapshot {:?}: {}", path, e);
                }
            }
        }

        if deleted_count > 0 {
            info!(
                "Successfully cleaned up {} old snapshot files",
                deleted_count
            );
        }
    }

    fn dump_hashmap_snapshot(
        &self,
        map: &HashMap<UtxoId, u64>,
        data_dir: &PathBuf,
        block_height: u64,
    ) -> io::Result<()> {
        let snapshot_path = data_dir.join(format!("utxo_snapshot_{}.bin", block_height));
        let temp_path = data_dir.join(format!("utxo_snapshot_{}.tmp", block_height));

        info!(
            "Creating UTXO snapshot at block {} with {} UTXOs",
            block_height,
            map.len()
        );
        let start_time = std::time::Instant::now();

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, file); // Larger buffer

            writer.write_all(&(block_height as u32).to_le_bytes())?;

            let mut entries_written = 0u64;
            for (&utxo_id, &balance) in map.iter() {
                writer.write_all(&utxo_id)?;
                writer.write_all(&balance.to_le_bytes())?;
                entries_written += 1;

                if entries_written % 10_000_000 == 0 {
                    info!(
                        "Snapshot progress: {} / {} UTXOs written",
                        entries_written,
                        map.len()
                    );
                }
            }

            writer.flush()?;
        }

        std::fs::rename(temp_path, snapshot_path)?;

        let duration = start_time.elapsed();
        info!(
            "UTXO snapshot completed in {:.2}s: {} UTXOs written",
            duration.as_secs_f64(),
            map.len()
        );

        // Clean up old snapshots after successful creation
        self.cleanup_old_snapshots(data_dir, block_height);

        Ok(())
    }

    pub fn load_balances_from_snapshot(&self, data_dir: &PathBuf) -> io::Result<Option<u64>> {
        let mut state = self.state.write().unwrap();
        match &mut *state {
            UtxoBalancesState::HashMap(map) => self.load_hashmap_from_snapshot(map, data_dir),
            UtxoBalancesState::RocksDB(_) => {
                info!("RocksDB snapshot loading not needed - data is already persistent");
                Ok(None)
            }
        }
    }

    fn load_hashmap_from_snapshot(
        &self,
        map: &mut HashMap<UtxoId, u64>,
        data_dir: &PathBuf,
    ) -> io::Result<Option<u64>> {
        let pattern = data_dir.join("utxo_snapshot_*.bin");
        let pattern_str = pattern.to_string_lossy();

        let entries = glob::glob(&pattern_str)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Glob error: {}", e)))?;

        let mut latest_snapshot: Option<(u64, PathBuf)> = None;

        for entry in entries {
            if let Ok(path) = entry {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        if let Some(captures) = regex::Regex::new(r"utxo_snapshot_(\d+)\.bin")
                            .unwrap()
                            .captures(file_name_str)
                        {
                            if let Some(height_str) = captures.get(1) {
                                if let Ok(height) = height_str.as_str().parse::<u64>() {
                                    if latest_snapshot.is_none()
                                        || height > latest_snapshot.as_ref().unwrap().0
                                    {
                                        latest_snapshot = Some((height, path));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let (snapshot_height, snapshot_path) = match latest_snapshot {
            Some(snapshot) => snapshot,
            None => {
                info!("No UTXO snapshot found");
                return Ok(None);
            }
        };

        info!("Loading UTXO snapshot from block {}", snapshot_height);
        let start_time = std::time::Instant::now();

        let file = File::open(&snapshot_path)?;
        let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file); // Larger buffer

        let mut height_bytes = [0u8; 4];
        reader.read_exact(&mut height_bytes)?;
        let file_block_height = u32::from_le_bytes(height_bytes) as u64;

        if file_block_height != snapshot_height {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Snapshot file block height mismatch: expected {}, found {}",
                    snapshot_height, file_block_height
                ),
            ));
        }

        map.clear();
        map.reserve(10_000_000); // Reserve space to avoid rehashing

        let mut entries_loaded = 0u64;
        let mut buffer = [0u8; 16];

        loop {
            match reader.read_exact(&mut buffer) {
                Ok(()) => {
                    let mut utxo_id = [0u8; 8];
                    utxo_id.copy_from_slice(&buffer[0..8]);

                    let balance = u64::from_le_bytes([
                        buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13],
                        buffer[14], buffer[15],
                    ]);

                    map.insert(utxo_id, balance);
                    entries_loaded += 1;

                    if entries_loaded % 10_000_000 == 0 {
                        info!("Snapshot loading progress: {} UTXOs loaded", entries_loaded);
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let duration = start_time.elapsed();
        info!(
            "UTXO snapshot loaded in {:.2}s: {} UTXOs loaded",
            duration.as_secs_f64(),
            entries_loaded
        );

        Ok(Some(snapshot_height))
    }

    pub fn load_all_movements_from_files(
        &self,
        sqlite_conn: &Mutex<Connection>,
        data_dir: &PathBuf,
        last_block_height: &RwLock<u64>,
    ) {
        let state = self.state.read().unwrap();
        match &*state {
            UtxoBalancesState::HashMap(_) => {
                drop(state);
                info!("HashMap mode detected - will load from snapshots and binary files");
                self.load_movements_for_hashmap(sqlite_conn, data_dir, last_block_height);
            }
            UtxoBalancesState::RocksDB(_) => {
                info!("RocksDB mode detected - data already persistent, skipping file loading");

                let sqlite_conn = sqlite_conn.lock().unwrap();
                let last_height = sqlite_conn
                    .query_row("SELECT MAX(height) FROM blocks", [], |row| {
                        row.get::<_, Option<i64>>(0)
                    })
                    .expect("Failed to query last block height")
                    .map(|height| height as u64)
                    .unwrap_or(0);
                drop(sqlite_conn);

                if last_height > 0 {
                    let mut last_height_guard = last_block_height.write().unwrap();
                    *last_height_guard = last_height;
                    info!("Restored last block height to {} from SQLite (RocksDB mode)", last_height);
                }
            }
        }
    }

    fn load_movements_for_hashmap(
        &self,
        sqlite_conn: &Mutex<Connection>,
        data_dir: &PathBuf,
        last_block_height: &RwLock<u64>,
    ) {
        info!("Loading UTXO state from binary files...");
        let total_start_time = std::time::Instant::now();

        let snapshot_height = match self.load_balances_from_snapshot(data_dir) {
            Ok(Some(height)) => {
                info!(
                    "Successfully loaded snapshot from block {}, will process remaining blocks",
                    height
                );
                height
            }
            Ok(None) => {
                info!("No snapshot found, will process all blocks from the beginning");
                0
            }
            Err(e) => {
                info!(
                    "Failed to load snapshot ({}), will process all blocks from the beginning",
                    e
                );
                0
            }
        };

        let row_mapper = |row: &rusqlite::Row| -> rusqlite::Result<(u64, u32, u64, u64)> {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u32,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
            ))
        };

        let block_info_vec = {
            let sqlite_conn = sqlite_conn.lock().unwrap();

            let query = if snapshot_height > 0 {
                "SELECT height, file_num, file_pos, movements_length FROM blocks WHERE height > ? ORDER BY height"
            } else {
                "SELECT height, file_num, file_pos, movements_length FROM blocks ORDER BY height"
            };

            let mut stmt = sqlite_conn
                .prepare(query)
                .expect("Failed to prepare blocks query");

            let block_rows = if snapshot_height > 0 {
                stmt.query_map([snapshot_height as i64], row_mapper)
            } else {
                stmt.query_map([], row_mapper)
            }
            .expect("Failed to execute blocks query");

            let mut blocks_vec = Vec::new();
            for row in block_rows {
                match row {
                    Ok(block_info) => blocks_vec.push(block_info),
                    Err(e) => {
                        panic!("Failed to read block row: {}", e);
                    }
                }
            }
            blocks_vec
        };

        if block_info_vec.is_empty() {
            if snapshot_height > 0 {
                info!(
                    "No additional blocks to process after snapshot at height {}",
                    snapshot_height
                );
                let mut last_height_guard = last_block_height.write().unwrap();
                *last_height_guard = snapshot_height;
                return;
            } else {
                info!("No blocks found in database");
                return;
            }
        }

        info!("Processing {} blocks after snapshot", block_info_vec.len());

        let mut movements_processed = 0u64;
        let mut last_height = snapshot_height;

        let mut balances = self.write_lock();

        for (height, file_num, file_pos, movements_length) in block_info_vec {
            last_height = height;

            let file_path = data_dir.join(format!("mhin_{:05}.bin", file_num));
            let mut file = File::open(&file_path).expect(&format!(
                "Failed to open binary file {}",
                file_path.display()
            ));

            file.seek(SeekFrom::Start(file_pos + 1 + 3))
                .expect(&format!(
                    "Failed to seek to movements in file {}",
                    file_path.display()
                ));

            read_varint(&mut file).expect("Failed to read movements length varint");

            let mut bytes_read = 0u64;
            while bytes_read < movements_length {
                let mut marker = [0u8; 1];
                file.read_exact(&mut marker)
                    .expect("Failed to read movement marker");
                bytes_read += 1;

                let mut utxo_id = [0u8; 8];
                file.read_exact(&mut utxo_id)
                    .expect("Failed to read UTXO ID");
                bytes_read += 8;

                let mut value_bytes = [0u8; 8];
                file.read_exact(&mut value_bytes)
                    .expect("Failed to read value");
                let value = u64::from_le_bytes(value_bytes);
                bytes_read += 8;

                match marker[0] {
                    ADD_MARKER => {
                        balances.insert(utxo_id, value);
                    }
                    POP_MARKER => {
                        balances.remove(&utxo_id);
                    }
                    _ => {
                        panic!("Unknown movement marker: {}", marker[0]);
                    }
                }

                movements_processed += 1;
            }

            if height % 1000 == 0 {
                print!(
                    "\rLoaded movements up to block {}, {} movements processed, {} UTXOs in memory",
                    height,
                    movements_processed,
                    balances.len()
                );
                let _ = io::stdout().flush();
            }
        }

        let final_utxo_count = balances.len();
        drop(balances);

        {
            let mut last_height_guard = last_block_height.write().unwrap();
            *last_height_guard = last_height;
        }

        let total_duration = total_start_time.elapsed();
        if snapshot_height > 0 {
            info!("UTXO state loading completed in {:.2}s: loaded snapshot up to block {}, processed {} additional movements, {} UTXOs total, last block: {}", 
                  total_duration.as_secs_f64(), snapshot_height, movements_processed, final_utxo_count, last_height);
        } else {
            info!("UTXO state loading completed in {:.2}s: {} movements processed, {} UTXOs loaded, last block: {}", 
                  total_duration.as_secs_f64(), movements_processed, final_utxo_count, last_height);
        }
    }
}