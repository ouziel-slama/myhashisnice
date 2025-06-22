// Constants for file operations
pub const MAX_FILE_SIZE: u64 = 500 * 1024 * 1024; // 500MB
pub const BLOCK_MARKER: u8 = b'B';
pub const ADD_MARKER: u8 = b'A';
pub const POP_MARKER: u8 = b'P';
pub const NICEHASHES_MARKER: u8 = b'N'; // New marker for nice hashes
