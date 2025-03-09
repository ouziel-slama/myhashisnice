import os
import struct
import binascii
import glob
from judy import JudyIntObjectMap


def h2b(h):
    """Convert hex string to bytes"""
    return binascii.unhexlify(h)


class MhinStorage:
    """
    Classe responsable de la gestion des fichiers .dat pour le stockage des blocs et des balances
    """

    def __init__(self, base_path):
        self.base_path = base_path
        self.balance_cache = JudyIntObjectMap()
        self.current_block = 0
        self.current_block_hash = None
        self.current_block_data = b""
        self.block_index = JudyIntObjectMap()

        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

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

        print(f"MhinStorage initialized. Current block: {self.current_block}")

    def _find_backup_files(self):
        """Search for all mhin_*.dat files in the base directory"""
        pattern = os.path.join(self.base_path, "mhin_*.dat")
        files = glob.glob(pattern)
        return sorted(files, key=self._extract_file_num)

    def _extract_file_num(self, file_path):
        """Extract file number from path"""
        basename = os.path.basename(file_path)
        try:
            return int(basename.split("_")[1].split(".")[0])
        except (IndexError, ValueError):
            return 0

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
                if record_type != b"B":
                    print(
                        f"Unexpected file format at position {block_start_pos}: {record_type}"
                    )
                    break

                # Read the block header
                try:
                    height_bytes = f.read(4)
                    block_hash = f.read(32)
                    length_bytes = f.read(8)

                    if (
                        len(height_bytes) != 4
                        or len(block_hash) != 32
                        or len(length_bytes) != 8
                    ):
                        print(f"Incomplete block header at position {block_start_pos}")
                        break

                    # Extract block height and length
                    height = struct.unpack("<I", height_bytes)[0]
                    block_length = struct.unpack("<Q", length_bytes)[0]

                    # Calculate expected end position of the block
                    expected_end_pos = f.tell() + block_length

                    # Check if the block is complete
                    if expected_end_pos > file_size:
                        print(
                            f"Incomplete block at position {block_start_pos}: "
                            f"expected {block_length} bytes but only "
                            f"{file_size - f.tell()} available"
                        )
                        break

                    # Skip the rest of the block
                    f.seek(expected_end_pos)

                    # Update truncation position
                    truncate_pos = expected_end_pos

                except Exception as e:
                    print(
                        f"Error while reading block at position {block_start_pos}: {e}"
                    )
                    break

            # If we haven't reached the end of the file, the file is corrupted
            if f.tell() < file_size:
                print(
                    f"Corrupted file at position {f.tell()}, truncating at {truncate_pos}"
                )
                f.truncate(truncate_pos)
            else:
                print(f"Valid file: {file_path}")

    def _restore_state_from_files(self, files):
        """Process all files in order to restore state"""
        for file_path in files:
            file_num = self._extract_file_num(file_path)
            with open(file_path, "rb") as f:
                self._process_backup_file(f, file_num)

    def _process_backup_file(self, file, file_num):
        """Process the content of a backup file"""
        print(f"Processing backup file: {file.name}")

        while file.tell() < os.path.getsize(file.name):
            pos = file.tell()  # Save position before reading

            # Read record type (1 byte)
            record_type = file.read(1)
            if not record_type:
                break  # End of file

            if record_type == b"B":  # Block start
                # Read the block height and hash
                height_bytes = file.read(4)
                block_hash = file.read(32)
                length_bytes = file.read(8)

                if (
                    len(height_bytes) != 4
                    or len(block_hash) != 32
                    or len(length_bytes) != 8
                ):
                    raise Exception(f"Incomplete block header at position {pos}")

                height = struct.unpack("<I", height_bytes)[0]
                block_length = struct.unpack("<Q", length_bytes)[0]
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

                    if inner_record_type == b"T":  # Transaction start
                        # Read transaction ID
                        txid = file.read(32)
                        if len(txid) != 32:
                            raise Exception(
                                f"Incomplete transaction ID at position {record_pos}"
                            )

                    elif inner_record_type == b"R":  # Transaction reward
                        # Read reward amount
                        reward_bytes = file.read(8)
                        if len(reward_bytes) != 8:
                            raise Exception(
                                f"Incomplete reward data at position {record_pos}"
                            )

                    elif inner_record_type == b"A":  # Balance addition
                        # Read UTXO ID and amount
                        utxo_id_bytes = file.read(8)
                        balance_bytes = file.read(8)

                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            raise Exception(
                                f"Incomplete balance data at position {record_pos}"
                            )

                        # Convert UTXO ID to little-endian integer to match add_balance
                        key = int.from_bytes(utxo_id_bytes, "little")
                        balance = struct.unpack("<Q", balance_bytes)[0]

                        # Update balance_cache
                        self.balance_cache[key] = balance

                    elif inner_record_type == b"P":  # Balance removal
                        # Read UTXO ID and amount
                        utxo_id_bytes = file.read(8)
                        balance_bytes = file.read(8)

                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            raise Exception(
                                f"Incomplete balance removal data at position {record_pos}"
                            )

                        # Convert UTXO ID to little-endian integer to match pop_balance
                        key = int.from_bytes(utxo_id_bytes, "little")
                        balance = struct.unpack("<Q", balance_bytes)[0]

                        # Update balance_cache
                        self.balance_cache.pop(key, 0)
                    else:
                        # Unknown record type
                        raise Exception(
                            f"Unknown record type: {inner_record_type} at position {record_pos}"
                        )

                # Go to the end of the block to move to the next block
                file.seek(block_end_pos)
            else:
                # Unknown record type at the root level of the file
                raise Exception(
                    f"Unknown root record type: {record_type} at position {pos}"
                )

    def start_block(self, height, block_hash):
        """Start a new block"""
        self.current_block = height
        self.block_index[height] = (
            self.current_backup_file_num,
            self.backup_file.tell(),
        )
        self.current_block_hash = block_hash
        self.current_block_data = b""

    def start_transaction(self, txid, reward):
        """Start a transaction within the current block"""
        self.current_block_data += struct.pack("<c32s", b"T", txid)
        if reward > 0:
            self.current_block_data += struct.pack("<cQ", b"R", reward)

    def add_balance(self, utxo_id, balance):
        """Add balance for a UTXO"""
        key = int.from_bytes(utxo_id, "little")
        self.balance_cache[key] = balance
        self.current_block_data += struct.pack("<c8sQ", b"A", utxo_id, balance)

    def pop_balance(self, utxo_id):
        """Remove balance for a UTXO"""
        key = int.from_bytes(utxo_id, "little")
        balance = self.balance_cache.pop(key, 0)
        if balance > 0:
            self.current_block_data += struct.pack("<c8sQ", b"P", utxo_id, balance)
        return balance

    def end_block(self):
        """Finalize the current block and write to file"""
        self._set_current_backup_file()
        self.current_block_data = (
            struct.pack(
                "<cI32sQ",
                b"B",
                self.current_block,
                h2b(self.current_block_hash),
                len(self.current_block_data),
            )
            + self.current_block_data
        )
        self.backup_file.write(self.current_block_data)
        self.backup_file.flush()  # Ensure data is written to disk
        self.current_block_data = b""

        # Return info about this block for the database
        return {
            "height": self.current_block,
            "hash": self.current_block_hash,
            "file_number": self.current_backup_file_num,
            "position": self.backup_file.tell() - len(self.current_block_data),
        }

    def _set_current_backup_file(self):
        """Rotate backup file if necessary"""
        if self.backup_file.tell() > 500 * 1024 * 1024:  # 500MB limit
            self.current_backup_file_num += 1
            self.backup_file.close()
            self.backup_file_path = (
                f"{self.base_path}/mhin_{self.current_backup_file_num}.dat"
            )
            self.backup_file = open(self.backup_file_path, "ab")

    def rollback_block(self, height):
        """Rollback a block"""
        if height != self.current_block:
            print(
                f"Cannot rollback: requested height {height} doesn't match current block {self.current_block}"
            )
            return False

        # Get information about the block to rollback
        file_num, position = self.block_index.get(height, (None, None))
        if file_num is None:
            print(f"Cannot rollback: block {height} not found in block_index")
            return False

        file_path = f"{self.base_path}/mhin_{file_num}.dat"
        try:
            with open(file_path, "rb") as f:
                f.seek(position)

                # Read block header
                record_type = f.read(1)
                if record_type != b"B":
                    print(
                        f"Cannot rollback: invalid record type at position {position}"
                    )
                    return False

                height_bytes = f.read(4)
                block_hash_bytes = f.read(32)
                length_bytes = f.read(8)

                if (
                    len(height_bytes) != 4
                    or len(block_hash_bytes) != 32
                    or len(length_bytes) != 8
                ):
                    print(
                        f"Cannot rollback: incomplete block header at position {position}"
                    )
                    return False

                # Verify height
                block_height = struct.unpack("<I", height_bytes)[0]
                if block_height != height:
                    print(
                        f"Cannot rollback: block height mismatch {block_height} != {height}"
                    )
                    return False

                block_length = struct.unpack("<Q", length_bytes)[0]
                block_end_pos = f.tell() + block_length

                # Read block operations to revert them
                rollback_operations = []

                while f.tell() < block_end_pos:
                    record_pos = f.tell()
                    inner_record_type = f.read(1)

                    if not inner_record_type:
                        print(f"Unexpected end of file at position {record_pos}")
                        return False

                    if inner_record_type == b"T":  # Transaction start
                        txid = f.read(32)
                        if len(txid) != 32:
                            print(f"Incomplete transaction ID at position {record_pos}")
                            return False

                    elif inner_record_type == b"R":  # Reward
                        reward_bytes = f.read(8)
                        if len(reward_bytes) != 8:
                            print(f"Incomplete reward data at position {record_pos}")
                            return False

                    elif inner_record_type == b"A":  # Balance addition
                        utxo_id_bytes = f.read(8)
                        balance_bytes = f.read(8)

                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            print(f"Incomplete balance data at position {record_pos}")
                            return False

                        # Store operation to revert
                        key = int.from_bytes(utxo_id_bytes, "little")
                        rollback_operations.append(("remove", key))

                    elif inner_record_type == b"P":  # Balance removal
                        utxo_id_bytes = f.read(8)
                        balance_bytes = f.read(8)

                        if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                            print(
                                f"Incomplete balance removal data at position {record_pos}"
                            )
                            return False

                        # Store operation to revert
                        key = int.from_bytes(utxo_id_bytes, "little")
                        balance = struct.unpack("<Q", balance_bytes)[0]
                        rollback_operations.append(("add", key, balance))

                # Reverse operations order for rollback
                rollback_operations.reverse()

                # Apply reverse operations
                for op in rollback_operations:
                    if op[0] == "remove":
                        key = op[1]
                        if key in self.balance_cache:
                            del self.balance_cache[key]
                    elif op[0] == "add":
                        key, balance = op[1], op[2]
                        self.balance_cache[key] = balance

                # Remove block entry from index
                if height in self.block_index:
                    del self.block_index[height]

                # Find previous block
                prev_height = height - 1
                if prev_height in self.block_index:
                    prev_file_num, prev_position = self.block_index[prev_height]

                    # Open previous block file
                    prev_file_path = f"{self.base_path}/mhin_{prev_file_num}.dat"
                    with open(prev_file_path, "rb") as prev_f:
                        prev_f.seek(prev_position)

                        # Read previous block header
                        prev_f.read(1)  # Skip record type
                        prev_f.read(4)  # Skip height
                        prev_block_hash_bytes = prev_f.read(32)

                        if len(prev_block_hash_bytes) == 32:
                            # Update current_block_hash
                            self.current_block_hash = binascii.hexlify(
                                prev_block_hash_bytes
                            ).decode("utf-8")

                # Update current_block
                self.current_block = prev_height

                # Truncate file at block position if it's the last block
                if file_path == self.backup_file_path:
                    self.backup_file.close()
                    with open(file_path, "r+b") as truncate_f:
                        truncate_f.truncate(position)
                    self.backup_file = open(file_path, "ab")

                print(f"Successfully rolled back block {height}")
                return rollback_operations

        except Exception as e:
            print(f"Error during rollback: {e}")
            import traceback

            print(traceback.format_exc())
            return False

    def close(self):
        """Close resources"""
        if self.backup_file:
            self.backup_file.close()
