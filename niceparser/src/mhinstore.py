import os
import struct
import binascii
import glob
from judy import JudyIntObjectMap

def h2b(h):
    return binascii.unhexlify(h)

class MhinStore:
    def __init__(self, base_path):
        self.base_path = base_path
        self.balance_cache = JudyIntObjectMap()
        self.current_block = 0
        self.current_block_hash = None
        self.current_block_data = b''
        
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)
        
        # Initialize block_index before restoring state
        self.block_index = JudyIntObjectMap()
        
        # Find all existing backup files
        files = self._find_backup_files()
        
        # Restore state from existing files if files exist
        if files:
            self._validate_backup_file(files[-1])
            self._restore_state_from_files(files)
            # Find the last backup file number
            self.current_backup_file_num = self._extract_file_num(files[-1])
        else:
            # No existing files, start at 0
            self.current_backup_file_num = 0
        
        # Open the current backup file in append mode
        self.backup_file_path = f"{base_path}/mhin_{self.current_backup_file_num}.dat"
        self.backup_file = open(self.backup_file_path, "ab")

        print("MhinStore successfully initialized")
        print("Current backup file:", self.backup_file_path)
        print("Current block:", self.current_block)

    def _find_backup_files(self):
        # Search for all mhin_*.dat files in the base directory
        pattern = os.path.join(self.base_path, "mhin_*.dat")
        files = glob.glob(pattern)
        
        # Sort files by number
        return sorted(files, key=self._extract_file_num)
    
    def _extract_file_num(self, file_path):
        # Extract file number from path
        basename = os.path.basename(file_path)
        try:
            return int(basename.split("_")[1].split(".")[0])
        except (IndexError, ValueError):
            return 0
        
    def _restore_state_from_files(self, files):
        # Process all files in order
        for file_path in files:
            file_num = self._extract_file_num(file_path)
            with open(file_path, "rb") as f:
                self._process_backup_file(f, file_num)

    def _validate_backup_file(self, file_path):
        """
        Checks that each block in the file is complete.
        If an incomplete block is found at the end, truncates the file at the last complete block.
        """
        print(f"Validating backup file: {file_path}")
        
        with open(file_path, "rb+") as f:
            # Position to truncate the file if necessary
            truncate_pos = 0
            file_size = os.path.getsize(file_path)
            
            # If the file is empty, no validation needed
            if file_size == 0:
                print(f"Empty file: {file_path}")
                return
                
            # Go through the file block by block
            while f.tell() < file_size:
                block_start_pos = f.tell()
                
                # Read the record type
                record_type = f.read(1)
                if not record_type:
                    break
                    
                # If it's not a block start, it's a corrupted file
                if record_type != b'B':
                    print(f"Unexpected file format at position {block_start_pos}: {record_type}")
                    break
                
                # Read the block header
                try:
                    height_bytes = f.read(4)
                    block_hash = f.read(32)
                    length_bytes = f.read(8)
                    
                    if len(height_bytes) != 4 or len(block_hash) != 32 or len(length_bytes) != 8:
                        print(f"Incomplete block header at position {block_start_pos}")
                        break
                        
                    # Extract block height and length
                    height = struct.unpack('<I', height_bytes)[0]
                    block_length = struct.unpack('<Q', length_bytes)[0]
                    
                    print(f"Block found: height={height}, length={block_length}")
                    
                    # Calculate expected end position of the block
                    expected_end_pos = f.tell() + block_length
                    
                    # Check if the block is complete
                    if expected_end_pos > file_size:
                        print(f"Incomplete block at position {block_start_pos}: "
                              f"expected {block_length} bytes but only "
                              f"{file_size - f.tell()} available")
                        break
                        
                    # Skip the rest of the block
                    f.seek(expected_end_pos)
                    
                    # Update truncation position
                    truncate_pos = expected_end_pos
                    
                except Exception as e:
                    print(f"Error while reading block at position {block_start_pos}: {e}")
                    break
                    
            # If we haven't reached the end of the file, the file is corrupted
            if f.tell() < file_size:
                print(f"Corrupted file at position {f.tell()}, truncating at {truncate_pos}")
                f.truncate(truncate_pos)
            else:
                print(f"Valid file: {file_path}")

    def _process_backup_file(self, file, file_num):
        print("Processing backup file:", file.name)
        # Process the content of a backup file
        
        while file.tell() < os.path.getsize(file.name):
            pos = file.tell()  # Save position before reading
            
            # Read record type (1 byte)
            record_type = file.read(1)
            if not record_type:
                break  # End of file
                
            if record_type == b'B':  # Block start
                # Read the block height and hash
                height_bytes = file.read(4)
                block_hash = file.read(32)
                length_bytes = file.read(8)
                
                if len(height_bytes) != 4 or len(block_hash) != 32 or len(length_bytes) != 8:
                    raise Exception(f"Incomplete block header at position {pos}")

                height = struct.unpack('<I', height_bytes)[0]
                block_length = struct.unpack('<Q', length_bytes)[0]
                self.current_block = height
                
                # Update block_index
                self.block_index[height] = (file_num, pos)
                
                # Process block content
                block_end_pos = file.tell() + block_length
                
                while file.tell() < block_end_pos:
                    record_pos = file.tell()
                    inner_record_type = file.read(1)
                    
                    if not inner_record_type:
                        raise Exception(f"Unexpected end of file in block {height}")
                        
                    if inner_record_type == b'T':  # Transaction start
                        # Read transaction ID
                        txid = file.read(32)
                        if len(txid) != 32:
                            raise Exception(f"Incomplete transaction ID at position {record_pos}")
                            
                    elif inner_record_type == b'R':  # Transaction reward
                        # Read reward amount
                        reward_bytes = file.read(8)
                        if len(reward_bytes) != 8:
                            raise Exception(f"Incomplete reward data at position {record_pos}")
                            
                    elif inner_record_type in (b'A', b'M'):  # Balance addition
                        # Read UTXO ID and amount
                        utxo_id_bytes = file.read(8)
                        balance_bytes = file.read(8)
                        
                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            raise Exception(f"Incomplete balance data at position {record_pos}")
                            
                        # Convert UTXO ID to little-endian integer to match add_balance
                        key = int.from_bytes(utxo_id_bytes, 'little')
                        balance = struct.unpack('<Q', balance_bytes)[0]
                        
                        # Update balance_cache
                        self.balance_cache[key] = self.balance_cache.get(key, 0) + balance
                        
                    elif inner_record_type == b'P':  # Balance removal
                        # Read UTXO ID and amount
                        utxo_id_bytes = file.read(8)
                        balance_bytes = file.read(8)
                        
                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            raise Exception(f"Incomplete balance removal data at position {record_pos}")
                            
                        # Convert UTXO ID to little-endian integer to match pop_balance
                        key = int.from_bytes(utxo_id_bytes, 'little')
                        balance = struct.unpack('<Q', balance_bytes)[0]
                        
                        # Update balance_cache
                        self.balance_cache.pop(key, 0)
                    else:
                        # Unknown record type, read next byte
                        raise Exception(f"Unknown record type: {inner_record_type} at position {record_pos}")
                
                # Go to the end of the block to move to the next block
                file.seek(block_end_pos)
            else:
                # Unknown record type at the root level of the file
                raise Exception(f"Unknown root record type: {record_type} at position {pos}")

    def start_block(self, height, block_hash):
        self.current_block = height
        self.block_index[height] = (self.current_backup_file_num, self.backup_file.tell())
        self.current_block_hash = block_hash

    def start_transaction(self, txid, reward):
        self.current_block_data += struct.pack('<c32s', b'T', txid)
        if reward > 0:
            self.current_block_data += struct.pack('<cQ', b'R', reward)

    def add_balance(self, utxo_id, balance, movement_type):
        key = int.from_bytes(utxo_id, 'little')
        self.balance_cache[key] = self.balance_cache.get(key, 0) + balance
        prefix = b'A' if movement_type == 'reward' else b'M'
        self.current_block_data += struct.pack('<c8sQ', prefix, utxo_id, balance)

    def pop_balance(self, utxo_id):
        key = int.from_bytes(utxo_id, 'little')
        balance = self.balance_cache.pop(key, 0)
        if balance > 0:
            self.current_block_data += struct.pack('<c8sQ', b'P', utxo_id, balance)
        return balance
    
    def end_block(self):
        self._set_current_backup_file()
        self.current_block_data = struct.pack(
            '<cI32sQ', 
            b'B',
            self.current_block, 
            h2b(self.current_block_hash),
            len(self.current_block_data)
        ) + self.current_block_data
        self.backup_file.write(self.current_block_data)
        self.backup_file.flush()  # Ensure data is written to disk
        self.current_block_data = b''
        self.current_block = 0

    
    def _set_current_backup_file(self):
        if self.backup_file.tell() > 500 * 1024 * 1024:
            self.current_backup_file_num += 1
            self.backup_file.close()
            self.backup_file_path = f"{self.base_path}/mhin_{self.current_backup_file_num}.dat"
            self.backup_file = open(self.backup_file_path, "ab")