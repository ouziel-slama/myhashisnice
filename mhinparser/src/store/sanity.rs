use crate::rpc::make_rpc_call;
use crate::store::constants::{ADD_MARKER, BLOCK_MARKER, NICEHASHES_MARKER, POP_MARKER};
use crate::store::utils::{calc_varint_size, inverse_hash, read_varint};
use crate::store::MhinStore;

use log::{info, warn};
use rusqlite::params;
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use tokio;

impl MhinStore {
    /// Perform sanity check to ensure consistency across storage systems
    pub fn perform_sanity_check(&self) {
        // Get the last block height from SQLite
        let sqlite_last_block = self.get_last_block_height();

        // If there are no blocks, nothing to check
        if sqlite_last_block == 0 {
            info!("No blocks found in the database, skipping sanity check.");
            return;
        }

        // Check for missing blocks in the sequence
        let sqlite_conn = self.sqlite_conn.lock().unwrap();
        let (min_height, max_height, block_count) = sqlite_conn
            .query_row(
                "SELECT MIN(height), MAX(height), COUNT(*) FROM blocks",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)? as u64,
                    ))
                },
            )
            .expect("Failed to query block range from SQLite");
        drop(sqlite_conn);

        let expected_count = max_height - min_height + 1;
        if block_count != expected_count {
            panic!(
                "Missing blocks detected: found {} blocks but expected {} (range {}-{})",
                block_count, expected_count, min_height, max_height
            );
        }

        info!(
            "Block sequence check passed: {} blocks in range {}-{}",
            block_count, min_height, max_height
        );

        // Get the last block height from memory
        let memory_last_block = self.get_last_parsed_block_height();

        // Memory and SQLite must be at the same block height
        if let Some(memory_height) = memory_last_block {
            if memory_height != sqlite_last_block {
                panic!(
                    "Database inconsistency: SQLite last block is {}, but memory last block is {}",
                    sqlite_last_block, memory_height
                );
            }
        } else {
            panic!(
                "Database inconsistency: Memory has no last block height, but SQLite has block {}",
                sqlite_last_block
            );
        }

        info!(
            "Database consistency check passed: both SQLite and memory at block {}",
            sqlite_last_block
        );

        // Get the file info for the last block from SQLite
        let sqlite_conn = self.sqlite_conn.lock().unwrap();
        let (file_num, file_pos, movements_length) = sqlite_conn
            .query_row(
                "SELECT file_num, file_pos, movements_length FROM blocks WHERE height = ?1",
                params![sqlite_last_block as i64],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .expect("Failed to query last block file info from SQLite");
        drop(sqlite_conn); // Release the lock

        // Open the file to verify block presence
        let file_path = self.data_dir.join(format!("mhin_{:05}.bin", file_num));
        let mut file =
            File::open(&file_path).expect("Failed to open binary file for last block verification");

        // Try to seek to the expected block position
        file.seek(SeekFrom::Start(file_pos as u64))
            .expect("Failed to seek to expected block position in binary file");

        // Try to read and verify the block marker
        let mut marker_buf = [0u8; 1];
        match file.read_exact(&mut marker_buf) {
            Ok(_) => {
                if marker_buf[0] != BLOCK_MARKER {
                    panic!("Databases ahead of binary file: expected block marker at position {}, found byte 0x{:02x}", 
                        file_pos, marker_buf[0]);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                panic!("Databases ahead of binary file: expected block at position {} but file ends at {}", 
                    file_pos, file.metadata().map(|m| m.len()).unwrap_or(0));
            }
            Err(e) => {
                panic!("Failed to read block marker from binary file: {}", e);
            }
        }

        // Read and verify the block height (3 bytes)
        let mut height_buf = [0u8; 3];
        match file.read_exact(&mut height_buf) {
            Ok(_) => {
                // Convert height bytes to u64
                let mut height_bytes = [0u8; 4];
                height_bytes[0..3].copy_from_slice(&height_buf);
                let file_block_height = u32::from_le_bytes(height_bytes) as u64;

                if file_block_height != sqlite_last_block {
                    panic!("Databases ahead of binary file: expected block height {} but found {} in binary file", 
                        sqlite_last_block, file_block_height);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                panic!("Databases ahead of binary file: expected block height but file truncated");
            }
            Err(e) => {
                panic!("Failed to read block height from binary file: {}", e);
            }
        }

        info!(
            "Binary file consistency check passed: block {} found at expected position",
            sqlite_last_block
        );

        // Calculate the end position of this block to check for additional data
        let varint_size = calc_varint_size(movements_length as u64);
        let mut current_pos = file_pos as u64 + 1 + 3 + varint_size + movements_length as u64;

        // Skip to after the movements data
        if let Err(e) = file.seek(SeekFrom::Start(current_pos)) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                info!("Sanity check passed: databases and binary file are consistent");
                return;
            }
            panic!("Failed to seek to end of movements data: {}", e);
        }

        // Read and skip nice hashes section
        let mut marker_buf = [0u8; 1];
        match file.read_exact(&mut marker_buf) {
            Ok(_) => {
                if marker_buf[0] != NICEHASHES_MARKER {
                    panic!(
                        "Invalid nice hashes marker in binary file at position {}",
                        current_pos
                    );
                }
                current_pos += 1;

                // Read the number of nice hashes
                let nice_hashes_count = read_varint(&mut file)
                    .expect("Failed to read nice hashes count from binary file");

                let nice_hashes_varint_size = calc_varint_size(nice_hashes_count);
                current_pos += nice_hashes_varint_size + (nice_hashes_count * 32);

                // Skip the nice hashes data
                if let Err(e) = file.seek(SeekFrom::Start(current_pos)) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        info!("Sanity check passed: databases and binary file are consistent");
                        return;
                    }
                    panic!("Failed to skip nice hashes data in binary file: {}", e);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Sanity check passed: databases and binary file are consistent");
                return;
            }
            Err(e) => {
                panic!("Failed to read nice hashes marker from binary file: {}", e);
            }
        }

        // Check if there are additional blocks in the binary file
        let mut next_marker_buf = [0u8; 1];
        match file.read_exact(&mut next_marker_buf) {
            Ok(_) => {
                if next_marker_buf[0] == BLOCK_MARKER {
                    // Rule 3: Databases are behind binary file - truncate the binary file
                    info!("Binary file ahead of databases: found additional blocks beyond height {}. Truncating binary file...", 
                        sqlite_last_block);
                    self.truncate_binary_file(
                        file_num as u32,
                        &file_path,
                        file_pos,
                        movements_length,
                    );
                } else {
                    info!("Sanity check passed: databases and binary file are consistent");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Sanity check passed: databases and binary file are consistent");
            }
            Err(e) => {
                panic!(
                    "Failed to check for additional blocks in binary file: {}",
                    e
                );
            }
        }

        // Check for blockchain reorganizations
        self.check_reorg();
    }

    /// Truncate binary file at the end of the last valid block
    fn truncate_binary_file(
        &self,
        file_num: u32,
        file_path: &Path,
        file_pos: i64,
        movements_length: i64,
    ) {
        info!(
            "Truncating binary file {:?} at the end of the last valid block",
            file_path
        );

        // Calculate the end position of the last valid block
        // This includes:
        // 1. The block marker (1 byte)
        // 2. The height (3 bytes)
        // 3. The movements length varint (variable size)
        // 4. The actual movements data
        // 5. The nice hashes marker (1 byte)
        // 6. The nice hashes count varint (variable size)
        // 7. The nice hashes data (count * 32 bytes)

        let varint_size = calc_varint_size(movements_length as u64);
        let mut end_position = file_pos as u64 + 1 + 3 + varint_size + movements_length as u64;

        // Read the nice hashes to calculate their size
        if let Ok(mut file) = File::open(file_path) {
            if file.seek(SeekFrom::Start(end_position)).is_ok() {
                // Read nice hashes marker
                let mut marker_buf = [0u8; 1];
                if file.read_exact(&mut marker_buf).is_ok() && marker_buf[0] == NICEHASHES_MARKER {
                    end_position += 1;

                    // Read nice hashes count
                    if let Ok(nice_hashes_count) = read_varint(&mut file) {
                        let nice_hashes_varint_size = calc_varint_size(nice_hashes_count);
                        end_position += nice_hashes_varint_size + (nice_hashes_count * 32);
                    }
                }
            }
        }

        // Open the file for writing to truncate it
        let file = OpenOptions::new()
            .write(true)
            .open(file_path)
            .expect("Failed to open binary file for truncation");
        file.set_len(end_position)
            .expect("Failed to truncate binary file");

        info!(
            "Successfully truncated binary file at position {}",
            end_position
        );

        // If this is the current file, update the current file size
        let file_num_guard = self.current_file_num.lock().unwrap();
        if *file_num_guard == file_num {
            let mut file_size_guard = self.current_file_size.lock().unwrap();
            *file_size_guard = end_position;
            info!("Updated current file size to {}", end_position);
        }
    }

    /// Check for blockchain reorganizations and rollback if necessary
    fn check_reorg(&self) {
        // Run the async reorg check in a separate thread to avoid runtime conflicts
        loop {
            // Get the last block from the database
            let last_block_info = {
                let sqlite_conn = self.sqlite_conn.lock().unwrap();
                match sqlite_conn.query_row(
                    "SELECT height, block_hash FROM blocks ORDER BY height DESC LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                ) {
                    Ok((height, hash)) => Some((height as u64, hash)),
                    Err(_) => {
                        info!("No blocks in database, skipping reorg check");
                        return;
                    }
                }
            };

            if let Some((height, stored_hash)) = last_block_info {
                // Retry mechanism for RPC calls
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 5;
                const RETRY_DELAY_SECS: u64 = 2;

                let chain_hash = loop {
                    // Clone the RPC URL for this iteration
                    let rpc_url = self.config.rpc_url.clone();

                    // Create a new thread to run the async RPC call
                    let handle = std::thread::spawn(move || {
                        // Create a new tokio runtime just for this RPC call
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            make_rpc_call(&rpc_url, "getblockhash", json!([height])).await
                        })
                    });

                    // Wait for the thread to complete and get the result
                    let rpc_result = handle.join().unwrap();

                    match rpc_result {
                        Some(value) => {
                            if let Some(hash) = value.as_str() {
                                break hash.to_string();
                            } else {
                                panic!("Invalid response format from RPC - expected string hash but got: {:?}", value);
                            }
                        }
                        None => {
                            retry_count += 1;
                            if retry_count >= MAX_RETRIES {
                                panic!("Failed to get block hash from RPC for height {} after {} retries", height, MAX_RETRIES);
                            }
                            warn!(
                                "RPC call failed for height {} (attempt {}/{}), retrying in {}s...",
                                height, retry_count, MAX_RETRIES, RETRY_DELAY_SECS
                            );
                            std::thread::sleep(std::time::Duration::from_secs(RETRY_DELAY_SECS));
                            continue;
                        }
                    }
                };

                // Check if there's a reorg
                if stored_hash == inverse_hash(&chain_hash) {
                    info!("Block {} hash matches chain, no reorg detected", height);
                    return;
                } else {
                    warn!(
                        "Reorg detected at block {}! Stored hash: {}, Chain hash: {}",
                        height, stored_hash, chain_hash
                    );
                    self.rollback_last_block();
                    // Continue the loop to check the previous block
                }
            } else {
                return;
            }
        }
    }

    /// Rollback the last block from all storage systems
    fn rollback_last_block(&self) {
        // Get the last block information
        let mut sqlite_conn = self.sqlite_conn.lock().unwrap();
        let tx = sqlite_conn
            .transaction()
            .expect("Failed to begin rollback transaction");

        // Get the last block details
        let (height, file_num, file_pos, movements_length) = tx.query_row(
            "SELECT height, file_num, file_pos, movements_length FROM blocks ORDER BY height DESC LIMIT 1",
            [],
            |row| Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u32,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64
            ))
        ).expect("Failed to get last block for rollback");

        info!("Rolling back block {}", height);

        let supply_change = tx
            .query_row(
                "SELECT SUM(reward) FROM nicehashes WHERE block_height = ?1",
                params![height as i64],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);

        // Get nice hashes count for this block to reverse stats
        let nice_hashes_count = tx
            .query_row(
                "SELECT COUNT(*) FROM nicehashes WHERE block_height = ?1",
                params![height as i64],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);

        // Read movements from binary file to reverse them
        let file_path = self.data_dir.join(format!("mhin_{:05}.bin", file_num));
        let mut file = File::open(&file_path).expect("Failed to open binary file for rollback");

        // Seek to block position and skip header
        file.seek(SeekFrom::Start(file_pos + 1 + 3))
            .expect("Failed to seek to movements in binary file");

        // Skip the movements length varint
        read_varint(&mut file).expect("Failed to read movements length varint");

        // Track movement statistics for rollback
        let mut add_count = 0i64;
        let mut pop_count = 0i64;

        // Read and reverse all movements in memory using UtxoBalances with write lock
        {
            let mut balances = self.utxo_balances.write_lock();
            let mut bytes_read = 0u64;
            while bytes_read < movements_length {
                // Read movement type marker
                let mut marker = [0u8; 1];
                file.read_exact(&mut marker)
                    .expect("Failed to read movement marker");
                bytes_read += 1;

                // Read UTXO ID
                let mut utxo_id = [0u8; 8];
                file.read_exact(&mut utxo_id)
                    .expect("Failed to read UTXO ID");
                bytes_read += 8;

                // Read value
                let mut value_bytes = [0u8; 8];
                file.read_exact(&mut value_bytes)
                    .expect("Failed to read value");
                let value = u64::from_le_bytes(value_bytes);
                bytes_read += 8;

                // Reverse the movement in memory and track stats
                match marker[0] {
                    ADD_MARKER => {
                        // Was an ADD, so we need to remove it
                        balances.remove(&utxo_id);
                        add_count += 1;
                    }
                    POP_MARKER => {
                        // Was a POP, so we need to add it back
                        balances.insert(utxo_id, value);
                        pop_count += 1;
                    }
                    _ => {
                        panic!("Unknown movement marker: {}", marker[0]);
                    }
                }
            }
        } // Write lock is automatically dropped here

        // Update the last block height in memory
        {
            let mut last_height = self.last_block_height.write().unwrap();
            if height > 0 {
                *last_height = height - 1;
            } else {
                *last_height = 0;
            }
        }

        // Reverse stats updates
        if nice_hashes_count > 0 {
            self.increment_stat(&tx, "nice_hashes_count", -nice_hashes_count)
                .expect("Failed to reverse nice_hashes_count stat during rollback");
        }

        if add_count > 0 {
            self.increment_stat(&tx, "unspent_utxo_id_count", -add_count)
                .expect("Failed to reverse unspent_utxo_id_count stat during rollback");
        }

        if pop_count > 0 {
            self.increment_stat(&tx, "spent_utxo_id_count", -pop_count)
                .expect("Failed to reverse spent_utxo_id_count stat during rollback");
            self.increment_stat(&tx, "unspent_utxo_id_count", pop_count)
                .expect("Failed to reverse unspent_utxo_id_count decrement during rollback");
        }

        if supply_change != 0 {
            self.increment_stat(&tx, "mhin_supply", -supply_change)
                .expect("Failed to reverse mhin_supply stat during rollback");
        }

        // Delete the block from SQLite (this will cascade delete nicehashes)
        tx.execute(
            "DELETE FROM blocks WHERE height = ?1",
            params![height as i64],
        )
        .expect("Failed to delete block from SQLite");

        // Commit SQLite transaction
        tx.commit()
            .expect("Failed to commit SQLite rollback transaction");

        // Truncate the binary file at the start of this block
        self.truncate_binary_file(file_num, &file_path, file_pos as i64, 0);

        info!("Successfully rolled back block {}", height);
    }
}
