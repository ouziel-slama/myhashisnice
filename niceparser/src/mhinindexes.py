import os
import struct
import binascii
import glob
import time
import apsw
import multiprocessing
import signal
from multiprocessing.sharedctypes import Value
import ctypes
from multiprocessing import Queue, Process
from queue import Empty

from nicefetcher import utils


def rowtracer(cursor, sql):
    """Converts fetched SQL data into dict-style"""
    return {
        name: (bool(value) if str(field_type) == "BOOL" else value)
        for (name, field_type), value in zip(cursor.getdescription(), sql)
    }


def apsw_connect(filename):
    db = apsw.Connection(filename)
    cursor = db.cursor()
    cursor.execute("PRAGMA page_size = 4096")
    cursor.execute("PRAGMA auto_vacuum = 0")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA journal_size_limit = 6144000")
    cursor.execute("PRAGMA cache_size = 10000")
    cursor.execute("PRAGMA defer_foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA locking_mode = NORMAL")

    db.setbusytimeout(5000)

    db.setrowtrace(rowtracer)
    cursor.close()
    return db


def count_zeros(hash_string):
    original_length = len(hash_string)
    stripped_length = len(hash_string.lstrip("0"))
    return original_length - stripped_length


def get_shard_id(utxo_id):
    """Determine the shard ID based on the first byte of utxo_id"""
    return utxo_id[0] % 10


# La fonction worker qui sera exécutée par chaque processus de shard
def shard_worker(shard_id, db_path, task_queue, result_queue, stop_event):
    """
    Fonction exécutée par chaque processus pour traiter les opérations sur un shard

    Args:
        shard_id (int): ID du shard à traiter
        db_path (str): Chemin vers la base de données du shard
        task_queue (Queue): File d'attente pour les tâches à exécuter
        result_queue (Queue): File d'attente pour les résultats
        stop_event (multiprocessing.Event): Événement pour signaler l'arrêt
    """
    try:
        # Connexion à la base de données du shard
        db = apsw_connect(db_path)
        cursor = db.cursor()

        print(f"Shard process {shard_id} started")

        while not stop_event.is_set():
            try:
                # Récupérer une tâche de la file d'attente avec un timeout
                task = task_queue.get(timeout=1)

                # Si la tâche est None, c'est un signal d'arrêt
                if task is None:
                    break

                height, operation, params = task

                # Exécuter l'opération dans une transaction
                with db:
                    if operation == "insert":
                        utxo_id_bytes, balance = params
                        cursor.execute(
                            "INSERT INTO balances (utxo_id, balance) VALUES (?, ?)",
                            (utxo_id_bytes, balance),
                        )
                    elif operation == "delete":
                        utxo_id_bytes = params
                        cursor.execute(
                            "DELETE FROM balances WHERE utxo_id = ?", (utxo_id_bytes,)
                        )

                # Signaler que la tâche est terminée - format cohérent: (height, shard_id, operation, success)
                result_queue.put((height, shard_id, operation, True))

            except Empty:
                # Continue si le timeout expire
                continue
            except Exception as e:
                print(f"Error in shard process {shard_id}: {e}")
                # Signaler l'erreur - format cohérent: (height, shard_id, operation, success, error_msg)
                result_queue.put((0, shard_id, "error", False, str(e)))

        db.close()
        print(f"Shard process {shard_id} stopped")

    except Exception as e:
        print(f"Fatal error in shard process {shard_id}: {e}")
        # Signaler une erreur fatale
        result_queue.put((0, shard_id, "fatal_error", False, str(e)))


class MhinIndexes:
    def __init__(self, mhin_store_base_path):
        self.mhin_store_base_path = mhin_store_base_path
        self.database_path = f"{mhin_store_base_path}/mhin_indexes.db"
        # Paths for the 10 sharded databases
        self.shard_database_paths = [
            f"{mhin_store_base_path}/mhin_balances_{i}.db" for i in range(10)
        ]

        # Initialize the databases
        self._initialize_database()

        # Create an event to signal process termination
        self.stop_event = multiprocessing.Event()

        # Process variable
        self.process = None

        # Shared variable for the last indexed block
        self.last_indexed_block = None

        # Adding queues and processes for parallel processing
        self.shard_queues = [None] * 10
        self.shard_processes = [None] * 10
        self.result_queue = None

    def _initialize_database(self):
        """Initialize the main database and sharded databases structure"""
        # Initialize the main database
        db = apsw_connect(self.database_path)
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                height INTEGER PRIMARY KEY,
                hash TEXT UNIQUE,
                file_number INTEGER,
                position INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS nicehashes (
                height INTEGER,
                txid TEXT UNIQUE,
                reward INTEGER
            );
            
            CREATE INDEX IF NOT EXISTS idx_nicehashes_height ON nicehashes (height);
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_number INTEGER PRIMARY KEY,
                position INTEGER
            );

            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            INSERT OR IGNORE INTO stats (key, value) VALUES ('supply', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('utxos_count', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('nice_hashes_count', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('nicest_hash', '');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('last_nice_hash', '');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('first_nice_hash', '000000329877c7141c6e50b04ed714a860abcb15135611f6ac92609cb392ef60');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('max_zero', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('last_parsed_block', '0');
        """)

        db.close()

        # Initialize each shard database
        for shard in range(10):
            shard_db = apsw_connect(self.shard_database_paths[shard])
            shard_cursor = shard_db.cursor()

            # Create balances table in each shard database
            shard_cursor.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    utxo_id BLOB PRIMARY KEY,
                    balance INTEGER
                );
                
                CREATE INDEX IF NOT EXISTS idx_balances_balance ON balances(balance);
            """)

            shard_db.close()

        # Initialize the stats with the total supply and utxos_count
        self._update_stats()

    def _update_stats(self):
        """Update the supply and utxos_count stats based on all sharded databases"""
        db = apsw_connect(self.database_path)
        cursor = db.cursor()

        total_supply = 0
        total_utxos = 0

        # Calculate totals from all shards
        for shard in range(10):
            shard_db = apsw_connect(self.shard_database_paths[shard])
            shard_cursor = shard_db.cursor()

            # Get supply from this shard
            supply_row = shard_cursor.execute(
                "SELECT COALESCE(SUM(balance), 0) as total FROM balances"
            ).fetchone()
            if supply_row:
                total_supply += supply_row["total"]

            # Get utxos count from this shard
            count_row = shard_cursor.execute(
                "SELECT COUNT(*) as count FROM balances"
            ).fetchone()
            if count_row:
                total_utxos += count_row["count"]

            shard_db.close()

        # Update the main database stats
        cursor.execute(
            "UPDATE stats SET value = ? WHERE key = 'supply'", (total_supply,)
        )
        cursor.execute(
            "UPDATE stats SET value = ? WHERE key = 'utxos_count'", (total_utxos,)
        )

        db.close()

    def start(self, last_indexed_block=None):
        """
        Start the monitoring process in a separate process

        Args:
            last_indexed_block (multiprocessing.Value, optional):
                Shared variable to monitor the last indexed block.
                If None, a new variable will be created.

        Returns:
            int: PID of the created process
        """
        if self.process is not None and self.process.is_alive():
            print("The monitoring process is already running")
            return self.process.pid

        # Create a shared variable for the last indexed block if not provided
        if last_indexed_block is None:
            last_indexed_block = Value(ctypes.c_uint32, 0)

        # Store the reference to the shared variable
        self.last_indexed_block = last_indexed_block

        # Reset the stop event
        self.stop_event.clear()

        # Initialize queues and processes for parallel processing
        self.result_queue = Queue()
        for shard_id in range(10):
            self.shard_queues[shard_id] = Queue()
            self.shard_processes[shard_id] = Process(
                target=shard_worker,
                args=(
                    shard_id,
                    self.shard_database_paths[shard_id],
                    self.shard_queues[shard_id],
                    self.result_queue,
                    self.stop_event,
                ),
                daemon=True,
            )
            self.shard_processes[shard_id].start()
            print(f"Started shard process {shard_id}")

        # Create and start the main monitoring process
        self.process = multiprocessing.Process(
            target=self._watch_process,
            args=(
                self.mhin_store_base_path,
                self.database_path,
                self.stop_event,
                last_indexed_block,
                self.shard_queues,
                self.result_queue,
            ),
            daemon=True,
        )

        self.process.start()
        print(f"Monitoring process started with PID {self.process.pid}")

        return self.process.pid

    def stop(self, timeout=10):
        """
        Gracefully stop the monitoring process

        Args:
            timeout (int): Maximum waiting time in seconds before forcing shutdown

        Returns:
            bool: True if the process stopped gracefully, False otherwise
        """
        if self.process is None or not self.process.is_alive():
            print("No monitoring process currently running")
            return True

        print(f"Stopping monitoring process (PID {self.process.pid})...")

        # Signal the process to stop
        self.stop_event.set()

        # Wait for the process to terminate
        self.process.join(timeout)

        # If the process hasn't terminated after timeout, force stop it
        if self.process.is_alive():
            print(f"Process not responding, forcing shutdown...")
            try:
                os.kill(self.process.pid, signal.SIGTERM)
                self.process.join(2)

                if self.process.is_alive():
                    os.kill(self.process.pid, signal.SIGKILL)
                    self.process.join(1)
            except OSError as e:
                print(f"Error during forced shutdown: {e}")

            if self.process.is_alive():
                print(f"Impossible to stop the process")
                return False

        # Stop shard processes
        for shard_id in range(10):
            if self.shard_queues[shard_id]:
                try:
                    self.shard_queues[shard_id].put(
                        None
                    )  # Signal for graceful shutdown
                except:
                    pass

            if (
                self.shard_processes[shard_id]
                and self.shard_processes[shard_id].is_alive()
            ):
                self.shard_processes[shard_id].join(2)
                if self.shard_processes[shard_id].is_alive():
                    try:
                        self.shard_processes[shard_id].terminate()
                    except:
                        pass

        print(f"Monitoring process stopped")
        self.process = None
        return True

    def get_last_indexed_block(self):
        """
        Get the height of the last indexed block

        Returns:
            int: Height of the last indexed block or 0 if no block has been indexed
        """
        if self.last_indexed_block is not None:
            return self.last_indexed_block.value
        return 0

    @staticmethod
    def _watch_process(
        mhin_store_base_path,
        database_path,
        stop_event,
        last_indexed_block,
        shard_queues,
        result_queue,
    ):
        """
        Function executed in a separate process to monitor files

        Args:
            mhin_store_base_path (str): Path to the MhinStore base directory
            database_path (str): Path to the main SQLite database
            stop_event (multiprocessing.Event): Event to signal process termination
            last_indexed_block (multiprocessing.Value): Shared variable for the last indexed block
            shard_queues (list): List of queues for each shard
            result_queue (Queue): Queue for results from shard processes
            instance (MhinIndexes): Reference to the instance for health checks
        """

        # Configure signal handlers
        def handle_signal(signum, frame):
            print(f"Signal {signum} received, shutting down...")
            stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        print(f"Monitoring process started for {mhin_store_base_path}")

        try:
            # Connect to the main database in this process
            db = apsw_connect(database_path)
            cursor = db.cursor()

            # Initialize a dictionary to track the read position in each file
            processed_files = {}

            # Retrieve information about already processed files
            for row in cursor.execute(
                "SELECT file_number, position FROM processed_files"
            ):
                processed_files[row["file_number"]] = row["position"]

            # Retrieve the last indexed block to initialize the shared variable
            max_height = cursor.execute(
                "SELECT MAX(height) as max_height FROM blocks"
            ).fetchone()
            if max_height and max_height["max_height"] is not None:
                with last_indexed_block.get_lock():
                    last_indexed_block.value = max_height["max_height"]

            # Monitor files until the stop event is triggered
            MhinIndexes._monitor_files(
                mhin_store_base_path,
                database_path,
                db,
                cursor,
                processed_files,
                stop_event,
                last_indexed_block,
                shard_queues,
                result_queue
            )

            # Close the connection
            db.close()

        except Exception as e:
            print(f"Error in the monitoring process: {e}")

        print(f"Monitoring process terminated")

    @staticmethod
    def _extract_file_num(file_path):
        """Extract the file number from the path"""
        basename = os.path.basename(file_path)
        try:
            return int(basename.split("_")[1].split(".")[0])
        except (IndexError, ValueError):
            return 0

    @staticmethod
    def _read_block_header(file):
        """
        Read a block header and check if it's complete

        Args:
            file: File opened in binary read mode

        Returns:
            tuple: (height, block_hash, block_length, True) if the header is complete
                   (None, None, None, False) if the header is incomplete
        """
        # Position at the beginning of the header
        header_start = file.tell()

        # Read the record type
        record_type = file.read(1)
        if not record_type or record_type != b"B":
            # Return to the initial position
            file.seek(header_start)
            return None, None, None, False

        # Read the block height and hash
        height_bytes = file.read(4)
        block_hash_bytes = file.read(32)
        length_bytes = file.read(8)

        if (
            len(height_bytes) != 4
            or len(block_hash_bytes) != 32
            or len(length_bytes) != 8
        ):
            # Incomplete header, return to the initial position
            file.seek(header_start)
            return None, None, None, False

        # Extract values
        height = struct.unpack("<I", height_bytes)[0]
        block_hash = binascii.hexlify(block_hash_bytes).decode("utf-8")
        block_length = struct.unpack("<Q", length_bytes)[0]

        return height, block_hash, block_length, True

    @staticmethod
    def _process_block(
        file,
        file_num,
        pos,
        cursor,
        last_indexed_block=None,
        shard_queues=None,
        result_queue=None,
    ):
        """
        Process a block in a file and update the database

        Args:
            file: File opened in binary read mode
            file_num (int): File number
            pos (int): Position in the file
            cursor: Main database cursor
            last_indexed_block (multiprocessing.Value, optional): Shared variable for the last indexed block
            shard_queues (list): List of queues for each shard
            result_queue (Queue): Queue for results from shard processes

        Returns:
            tuple: (new_position, success)
                new_position: Position after the block or unchanged if failure
                success: True if the block was processed, False otherwise
        """
        # Save the initial position
        initial_pos = pos
        file.seek(pos)
        nice_hash_count = 0
        last_nice_hash = None
        max_zero = int(
            cursor.execute("SELECT value FROM stats WHERE key = 'max_zero'").fetchone()[
                "value"
            ]
        )
        supply_delta = 0
        utxos_count_delta = 0

        # Dictionary to track operations for each shard
        shard_operations = {shard: {"insert": [], "delete": []} for shard in range(10)}

        # Read the block header
        height, block_hash, block_length, header_complete = (
            MhinIndexes._read_block_header(file)
        )

        # If the header is incomplete, the file might be in the process of being written
        if not header_complete:
            return pos, False

        # Calculate the end position of the block
        block_end_pos = file.tell() + block_length

        # Check if the block is complete in the file
        file_size = os.path.getsize(file.name)
        if block_end_pos > file_size:
            # The block is not complete, wait for it to be
            print(
                f"Block {height} incomplete. Requires {block_end_pos - file_size} additional bytes."
            )
            return pos, False

        # Check if the block has already been processed
        existing = cursor.execute(
            "SELECT height FROM blocks WHERE height = ?", (height,)
        ).fetchone()
        if existing:
            # If the block already exists, advance to the end of the block
            file.seek(block_end_pos)
            return block_end_pos, True

        try:
            # Insert block information into the blocks table
            cursor.execute(
                "INSERT INTO blocks (height, hash, file_number, position) VALUES (?, ?, ?, ?)",
                (height, block_hash, file_num, initial_pos),
            )

            # Variables to store information about the nicehash transaction
            current_txid = None
            current_reward = 0

            # Process the block content
            while file.tell() < block_end_pos:
                record_pos = file.tell()
                inner_record_type = file.read(1)

                if not inner_record_type:
                    raise Exception(f"Unexpected end of file at position {record_pos}")

                if inner_record_type == b"T":  # Transaction start
                    # Read the transaction ID
                    txid_bytes = file.read(32)
                    if len(txid_bytes) != 32:
                        raise Exception(
                            f"Incomplete transaction ID at position {record_pos}"
                        )

                    current_txid = utils.inverse_hash(
                        binascii.hexlify(txid_bytes).decode("utf-8")
                    )
                    current_reward = 0

                elif inner_record_type == b"R":  # Transaction reward
                    # Read the reward amount
                    reward_bytes = file.read(8)
                    if len(reward_bytes) != 8:
                        raise Exception(
                            f"Incomplete reward data at position {record_pos}"
                        )

                    current_reward = struct.unpack("<Q", reward_bytes)[0]

                    # If we have a txid and a reward, record in the nicehashes table
                    if current_txid:
                        nice_hash_count += 1
                        last_nice_hash = current_txid
                        cursor.execute(
                            "INSERT INTO nicehashes (height, txid, reward) VALUES (?, ?, ?)",
                            (height, current_txid, current_reward),
                        )
                        zero_count = count_zeros(current_txid)
                        if zero_count > max_zero:
                            cursor.execute(
                                "UPDATE stats SET value = ? WHERE key = 'max_zero'",
                                (zero_count,),
                            )
                            cursor.execute(
                                "UPDATE stats SET value = ? WHERE key = 'nicest_hash'",
                                (current_txid,),
                            )
                            max_zero = count_zeros(current_txid)
                        supply_delta += current_reward

                elif inner_record_type == b"A":  # Balance addition
                    # Read the UTXO ID and amount
                    utxo_id_bytes = file.read(8)
                    balance_bytes = file.read(8)

                    if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                        raise Exception(
                            f"Incomplete balance data at position {record_pos}"
                        )

                    balance = struct.unpack("<Q", balance_bytes)[0]

                    # Determine the shard ID
                    shard_id = get_shard_id(utxo_id_bytes)

                    # Add to insert operations for this shard
                    shard_operations[shard_id]["insert"].append(
                        (utxo_id_bytes, balance)
                    )
                    utxos_count_delta += 1

                elif inner_record_type == b"P":  # Balance removal
                    # Read the UTXO ID and amount
                    utxo_id_bytes = file.read(8)
                    balance_bytes = file.read(8)

                    if len(utxo_id_bytes) != 8 or len(balance_bytes) != 8:
                        raise Exception(
                            f"Incomplete balance removal data at position {record_pos}"
                        )

                    # Determine the shard ID and add to deletion list
                    shard_id = get_shard_id(utxo_id_bytes)
                    shard_operations[shard_id]["delete"].append(utxo_id_bytes)
                    utxos_count_delta -= 1

            # Process database operations for each shard via queues
            if shard_queues and result_queue:
                # Count total operations to wait for
                total_operations = 0
                pending_shards = []

                for shard_id, operations in shard_operations.items():
                    insert_ops = len(operations["insert"])
                    delete_ops = len(operations["delete"])

                    if insert_ops > 0 or delete_ops > 0:
                        pending_shards.append(shard_id)

                    # Send insert operations
                    for utxo_id_bytes, balance in operations["insert"]:
                        shard_queues[shard_id].put(
                            (height, "insert", (utxo_id_bytes, balance))
                        )
                        total_operations += 1

                    # Send delete operations
                    for utxo_id_bytes in operations["delete"]:
                        shard_queues[shard_id].put((height, "delete", utxo_id_bytes))
                        total_operations += 1

                # Wait for all operations to complete
                completed_operations = 0
                operation_status = True
                pending_results = {}

                # Initialize tracking for each shard
                for shard_id in range(10):  # Initialiser tous les shards possibles
                    pending_results[shard_id] = {
                        "total": 0,
                        "completed": 0,
                        "success": True,
                    }

                # Count total operations per shard
                for shard_id, operations in shard_operations.items():
                    pending_results[shard_id]["total"] += len(
                        operations["insert"]
                    ) + len(operations["delete"])

                # Wait for all results with timeout
                while completed_operations < total_operations:
                    try:
                        # Wait for a result with timeout
                        result = result_queue.get(timeout=10)

                        # Unpack the result, avec gestion des résultats à 4 ou 5 éléments
                        if len(result) >= 4:
                            block_height, shard_id, operation, success = result[:4]
                            error_msg = result[4] if len(result) > 4 else None
                        else:
                            # Format de résultat invalide
                            print(f"Invalid result format: {result}")
                            operation_status = False
                            break

                        # Track completed operations
                        if shard_id in pending_results:
                            pending_results[shard_id]["completed"] += 1

                            # Track failures
                            if not success:
                                pending_results[shard_id]["success"] = False
                                print(
                                    f"Error in shard {shard_id} operation: {error_msg if error_msg else 'Unknown error'}"
                                )
                                operation_status = False

                        completed_operations += 1

                    except Exception as e:
                        print(f"Error waiting for shard operations: {e}")
                        # Don't immediately fail, try to recover
                        if completed_operations > 0:
                            print(
                                f"Continuing with {completed_operations}/{total_operations} completed operations"
                            )
                            break
                        else:
                            return initial_pos, False

                # Check if all operations were successful
                if not operation_status:
                    print(
                        f"Some shard operations failed for block {height}, rolling back"
                    )
                    return initial_pos, False
            else:
                # Fallback to original sequential processing if no queues are provided
                print(
                    "Warning: No shard queues provided, falling back to sequential processing"
                )
                return initial_pos, False

            # Ensure advancing to the end of the block
            file.seek(block_end_pos)

            # Update the shared variable with the block height
            if last_indexed_block is not None:
                with last_indexed_block.get_lock():
                    last_indexed_block.value = max(height, last_indexed_block.value)

            # update nice_hashes_count
            if nice_hash_count > 0:
                cursor.execute(
                    "UPDATE stats SET value = value + ? WHERE key = 'nice_hashes_count'",
                    (nice_hash_count,),
                )
            if last_nice_hash is not None:
                cursor.execute(
                    "UPDATE stats SET value = ? WHERE key = 'last_nice_hash'",
                    (last_nice_hash,),
                )
            if supply_delta > 0:
                cursor.execute(
                    "UPDATE stats SET value = value + ? WHERE key = 'supply'",
                    (supply_delta,),
                )
            if utxos_count_delta != 0:
                cursor.execute(
                    "UPDATE stats SET value = value + ? WHERE key = 'utxos_count'",
                    (utxos_count_delta,),
                )
            cursor.execute(
                "UPDATE stats SET value = ? WHERE key = 'last_parsed_block'", (height,)
            )

            return block_end_pos, True

        except Exception as e:
            import traceback

            print(f"Error while processing block at position {pos}: {e}")
            print(f"Error type: {type(e)}")
            print(traceback.format_exc())
            # In case of error, return to the initial position
            file.seek(initial_pos)
            return initial_pos, False

    @staticmethod
    def _monitor_files(
        mhin_store_base_path,
        database_path,
        db,
        cursor,
        processed_files,
        stop_event,
        last_indexed_block=None,
        shard_queues=None,
        result_queue=None,
    ):
        """
        Monitor .dat files from MhinStore and populate the database

        Args:
            mhin_store_base_path (str): Path to the base directory
            database_path (str): Path to the main SQLite database
            db (apsw.Connection): Main database connection
            cursor: Main database cursor
            processed_files (dict): Dictionary of processed positions
            stop_event (multiprocessing.Event): Event to signal shutdown
            last_indexed_block (multiprocessing.Value, optional): Shared variable for the last indexed block
            shard_queues (list): List of queues for each shard
            result_queue (Queue): Queue for results from shard processes
            instance (MhinIndexes): Reference to the MhinIndexes instance for health checks
        """
        print(f"Starting monitoring of .dat files in {mhin_store_base_path}")

        while not stop_event.is_set():
            try:
                # Find all mhin_*.dat files in the base directory
                pattern = os.path.join(mhin_store_base_path, "mhin_*.dat")
                files = sorted(glob.glob(pattern), key=MhinIndexes._extract_file_num)

                if not files:
                    print("No .dat files found, checking again in 5 seconds")
                    # Wait before checking again, but regularly check the stop event
                    for _ in range(5):
                        if stop_event.is_set():
                            break
                        time.sleep(1)
                    continue

                blocks_processed = 0

                for file_path in files:
                    if stop_event.is_set():
                        break

                    file_num = MhinIndexes._extract_file_num(file_path)

                    # Determine the starting position in the file
                    start_pos = processed_files.get(file_num, 0)

                    # Calculate file size
                    file_size = os.path.getsize(file_path)

                    # Process the file if it hasn't been fully processed
                    if start_pos < file_size:
                        with open(file_path, "rb") as f:
                            # Process blocks until the end of file or until an incomplete block
                            current_pos = start_pos

                            while current_pos < file_size and not stop_event.is_set():
                                # Process the block inside a SQL transaction
                                with db:
                                    try:
                                        new_pos, success = MhinIndexes._process_block(
                                            f,
                                            file_num,
                                            current_pos,
                                            cursor,
                                            last_indexed_block,
                                            shard_queues,
                                            result_queue,
                                        )

                                        if not success:
                                            # Block is incomplete or corrupted, stop
                                            break

                                        # Update the position
                                        if new_pos > current_pos:
                                            current_pos = new_pos
                                            blocks_processed += 1

                                            # Update the processed position for this file
                                            processed_files[file_num] = current_pos
                                            cursor.execute(
                                                "INSERT OR REPLACE INTO processed_files (file_number, position) VALUES (?, ?)",
                                                (file_num, current_pos),
                                            )

                                        else:
                                            # Unlikely case: position unchanged but success
                                            break
                                    except Exception as e:
                                        print(f"Error processing block: {e}")
                                        raise e

                # If blocks have been processed, wait less time before the next check
                if blocks_processed > 0:
                    wait_time = 1
                else:
                    wait_time = 5

                # Wait before checking again, while checking the stop event
                for _ in range(wait_time):
                    if stop_event.is_set():
                        break
                    time.sleep(1)

            except Exception as e:
                print(f"Error while monitoring files: {e}")
                # Wait longer in case of error, but check the stop event
                for _ in range(30):
                    if stop_event.is_set():
                        break
                    time.sleep(1)

        print("Monitoring stopped following a stop request")
