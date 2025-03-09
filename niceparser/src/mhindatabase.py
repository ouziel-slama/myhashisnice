import os
import threading
import apsw
import binascii
import time
import traceback
from multiprocessing import Queue, Process, Event
from queue import Empty

from nicefetcher import utils


def rowtracer(cursor, sql):
    """Converts fetched SQL data into dict-style"""
    return {
        name: (bool(value) if str(field_type) == "BOOL" else value)
        for (name, field_type), value in zip(cursor.getdescription(), sql)
    }


def apsw_connect(filename):
    """Connect to SQLite database with optimal settings"""
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
    """Count leading zeros in hash string"""
    # Convert to string if it's bytes
    if isinstance(hash_string, bytes):
        try:
            # Try to convert bytes to hex string
            hash_string = binascii.hexlify(hash_string).decode("ascii")
        except:
            # If we can't convert directly, try to handle it as is
            hash_string = str(hash_string)
    hash_string = utils.inverse_hash(hash_string)

    # Now count the leading zeros
    original_length = len(hash_string)
    stripped_length = len(hash_string.lstrip("0"))
    return original_length - stripped_length


def get_shard_id(utxo_id):
    """Determine the shard ID based on the first byte of utxo_id"""
    return utxo_id[0] % 10


# Work function for the shard process
# Remplacer la fonction shard_worker_process dans mhindatabase.py

def shard_worker_process(shard_id, db_path, task_queue, result_queue, stop_event):
    """Process function for shard worker"""
    
    # Ignorer les interruptions SIGINT dans le processus enfant pour
    # laisser le processus parent s'en occuper proprement
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to the database
        db = apsw_connect(db_path)
        cursor = db.cursor()

        # Create the tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balances (
                utxo_id BLOB PRIMARY KEY,
                balance INTEGER
            );
            
            CREATE INDEX IF NOT EXISTS idx_balances_balance ON balances(balance);
        """)

        print(f"Shard {shard_id} process started")

        # Batch processing variables
        batch = []
        batch_size = 1000  # Number of operations to batch
        last_batch_time = time.time()

        while not stop_event.is_set():
            try:
                # Try to get a task with timeout
                try:
                    # Utiliser un timeout plus court pour vérifier plus souvent stop_event
                    task = task_queue.get(timeout=0.5)
                except Empty:
                    # Process any pending batch if it's been a while
                    current_time = time.time()
                    if batch and current_time - last_batch_time > 5:
                        try:
                            with db:
                                for query, params in batch:
                                    cursor.execute(query, params)
                            batch = []
                            last_batch_time = current_time
                        except Exception as e:
                            print(f"Error processing batch in shard {shard_id}: {e}")
                            batch = []  # Réinitialiser le batch en cas d'erreur
                    continue

                # None est un signal d'arrêt
                if task is None:
                    print(f"Shard {shard_id} received stop signal")
                    break

                task_type, args = task

                if task_type == "batch_operations":
                    # Traiter un lot d'opérations en une seule fois
                    batch = []
                    for op_type, op_args in args:
                        if op_type == "add_balance":
                            utxo_id, balance = op_args
                            batch.append(
                                (
                                    "INSERT OR REPLACE INTO balances (utxo_id, balance) VALUES (?, ?)",
                                    (utxo_id, balance),
                                )
                            )
                        elif op_type == "remove_balance":
                            utxo_id = op_args[0]
                            batch.append(("DELETE FROM balances WHERE utxo_id = ?", (utxo_id,)))
                    
                    # Exécuter le lot immédiatement
                    if batch:
                        try:
                            with db:
                                for query, params in batch:
                                    cursor.execute(query, params)
                            batch = []
                            last_batch_time = time.time()
                        except Exception as e:
                            print(f"Error processing batch operation in shard {shard_id}: {e}")
                            # Envoyer l'erreur au processus principal
                            result_queue.put(("error", (shard_id, str(e))))
                            batch = []  # Réinitialiser le batch en cas d'erreur

                elif task_type == "add_balance":
                    utxo_id, balance = args
                    batch.append(
                        (
                            "INSERT OR REPLACE INTO balances (utxo_id, balance) VALUES (?, ?)",
                            (utxo_id, balance),
                        )
                    )

                elif task_type == "remove_balance":
                    utxo_id = args
                    batch.append(("DELETE FROM balances WHERE utxo_id = ?", (utxo_id,)))

                # Process batch if full or if it's been a while
                current_time = time.time()
                if len(batch) >= batch_size or (
                    batch and current_time - last_batch_time > 5
                ):
                    try:
                        with db:
                            for query, params in batch:
                                cursor.execute(query, params)
                        batch = []
                        last_batch_time = current_time
                    except Exception as e:
                        print(f"Error processing batch in shard {shard_id}: {e}")
                        # Envoyer l'erreur au processus principal
                        result_queue.put(("error", (shard_id, str(e))))
                        batch = []  # Réinitialiser le batch en cas d'erreur

                # Vérifier régulièrement si un arrêt est demandé
                if stop_event.is_set():
                    print(f"Shard {shard_id} detected stop event")
                    break

            except KeyboardInterrupt:
                # Ignorer KeyboardInterrupt, c'est au processus parent de s'en occuper
                print(f"Shard {shard_id} ignoring KeyboardInterrupt")
                continue
            except Empty:
                # Traiter les interruptions de queue.get comme des erreurs bénignes
                continue
            except Exception as e:
                print(f"Error in shard {shard_id} worker: {e}")
                import traceback
                traceback.print_exc()
                result_queue.put(("error", (shard_id, str(e))))

        # Process any remaining batch before closing
        if batch:
            try:
                print(f"Shard {shard_id} processing final batch before shutdown")
                with db:
                    for query, params in batch:
                        cursor.execute(query, params)
            except Exception as e:
                print(f"Error processing final batch in shard {shard_id}: {e}")

        # Close database connection
        try:
            db.close()
            print(f"Shard {shard_id} database closed successfully")
        except Exception as e:
            print(f"Error closing database in shard {shard_id}: {e}")
            
        print(f"Shard {shard_id} process stopped")

    except Exception as e:
        print(f"Fatal error in shard {shard_id} worker: {e}")
        import traceback
        traceback.print_exc()
        try:
            result_queue.put(("fatal_error", (shard_id, str(e))))
        except:
            pass  # Ignorer les erreurs lors de l'envoi final


class MhinDatabase:
    """
    Class to handle database operations for indexing
    """

    def __init__(self, base_path):
        self.base_path = base_path
        self.database_path = f"{base_path}/mhin_indexes.db"
        self.db = None
        self.last_indexed_block = 0

        # Process management
        self.stop_event = Event()
        self.shard_processes = {}
        self.shard_queues = {}
        self.result_queue = Queue()

        # Optimization: Buffers for operations
        self.operation_buffers = {shard_id: [] for shard_id in range(10)}
        self.buffer_max_size = 500  # Ajustable selon les performances
        self.buffer_flush_timer = time.time()
        self.buffer_max_age = 0.5  # Flush toutes les 500ms

        # Create directories if needed
        os.makedirs(base_path, exist_ok=True)

        # Initialize database
        self._initialize_database()

        # Start shard processes
        self._start_shard_processes()

        # Start result processing thread
        self.result_processor_stop = threading.Event()
        self.result_processor = threading.Thread(target=self._process_results)
        self.result_processor.daemon = True
        self.result_processor.start()

    def _initialize_database(self):
        """Initialize the main database structure"""
        self.db = apsw_connect(self.database_path)
        cursor = self.db.cursor()

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

        first_hash = utils.inverse_hash(
            "000000329877c7141c6e50b04ed714a860abcb15135611f6ac92609cb392ef60"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                file_number INTEGER PRIMARY KEY,
                position INTEGER
            );

            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            INSERT OR IGNORE INTO stats (key, value) VALUES ('supply', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('supply_check', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('utxos_count', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('nice_hashes_count', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('nicest_hash', '');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('last_nice_hash', '');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('first_nice_hash', ?);
            INSERT OR IGNORE INTO stats (key, value) VALUES ('max_zero', '0');
            INSERT OR IGNORE INTO stats (key, value) VALUES ('last_parsed_block', '0');
        """,
            (first_hash,),
        )

        # Get the last indexed block height
        row = cursor.execute("SELECT MAX(height) as height FROM blocks").fetchone()
        if row and row["height"] is not None:
            self.last_indexed_block = row["height"]

        print(
            f"MhinDatabase initialized. Last indexed block: {self.last_indexed_block}"
        )

    def _start_shard_processes(self):
        """Start all shard worker processes"""
        for shard_id in range(10):
            db_path = f"{self.base_path}/mhin_balances_{shard_id}.db"
            queue = Queue()

            process = Process(
                target=shard_worker_process,
                args=(shard_id, db_path, queue, self.result_queue, self.stop_event),
                daemon=True,
            )

            process.start()
            self.shard_processes[shard_id] = process
            self.shard_queues[shard_id] = queue

        print(f"Started {len(self.shard_processes)} shard processes")

    def _process_results(self):
        """Process results from shard workers"""
        pending_balance_requests = {}
        pending_stats_requests = set()
        stats_results = {}

        while not self.result_processor_stop.is_set():
            try:
                # Try to get a result with timeout
                try:
                    result = self.result_queue.get(timeout=0.1)
                except Empty:
                    continue

                result_type, data = result

                if result_type == "balance_result":
                    task_id, balance = data
                    if task_id in pending_balance_requests:
                        pending_balance_requests[task_id] = balance

                elif result_type == "stats_result":
                    shard_id, stats = data
                    stats_results[shard_id] = stats
                    pending_stats_requests.discard(shard_id)

                elif result_type == "error" or result_type == "fatal_error":
                    shard_id, error_msg = data
                    print(f"Error in shard {shard_id}: {error_msg}")
                    # Could implement retry logic here

            except Exception as e:
                print(f"Error processing results: {e}")
                print(traceback.format_exc())

        print("Result processor stopped")

    def _flush_operation_buffer(self, shard_id=None):
        """Flush les opérations en attente vers les shards"""
        current_time = time.time()
        
        # Si un shard spécifique est demandé
        if shard_id is not None and self.operation_buffers[shard_id]:
            if shard_id in self.shard_queues:
                self.shard_queues[shard_id].put(("batch_operations", self.operation_buffers[shard_id]))
            self.operation_buffers[shard_id] = []
            return
            
        # Sinon, vérifier tous les buffers
        for sid, buffer in self.operation_buffers.items():
            if buffer and (current_time - self.buffer_flush_timer > self.buffer_max_age):
                if sid in self.shard_queues:
                    self.shard_queues[sid].put(("batch_operations", buffer))
                self.operation_buffers[sid] = []
        
        # Mettre à jour le timer si on a flush
        if current_time - self.buffer_flush_timer > self.buffer_max_age:
            self.buffer_flush_timer = current_time

    def add_balance(self, utxo_id, balance):
        """Add or update a balance in the appropriate shard"""
        shard_id = get_shard_id(utxo_id)
        if shard_id in self.shard_queues:
            # Ajouter au buffer au lieu d'envoyer directement
            self.operation_buffers[shard_id].append(("add_balance", (utxo_id, balance)))
            
            # Flush si buffer plein
            if len(self.operation_buffers[shard_id]) >= self.buffer_max_size:
                self._flush_operation_buffer(shard_id)
        
        # Vérifier régulièrement les autres buffers
        if time.time() - self.buffer_flush_timer > self.buffer_max_age:
            self._flush_operation_buffer()

    def remove_balance(self, utxo_id):
        """Remove a balance from the appropriate shard"""
        shard_id = get_shard_id(utxo_id)
        if shard_id in self.shard_queues:
            # Ajouter au buffer au lieu d'envoyer directement
            self.operation_buffers[shard_id].append(("remove_balance", (utxo_id,)))
            
            # Flush si buffer plein
            if len(self.operation_buffers[shard_id]) >= self.buffer_max_size:
                self._flush_operation_buffer(shard_id)
        
        # Vérifier régulièrement les autres buffers
        if time.time() - self.buffer_flush_timer > self.buffer_max_age:
            self._flush_operation_buffer()

    def process_block(self, block_info, transactions):
        """Process a block and its transactions"""
        height = block_info["height"]
        block_hash = block_info["hash"]
        file_number = block_info["file_number"]
        position = block_info["position"]

        cursor = self.db.cursor()

        with self.db:  # Start a transaction for the main database
            # Insert block information
            cursor.execute(
                "INSERT INTO blocks (height, hash, file_number, position) VALUES (?, ?, ?, ?)",
                (height, block_hash, file_number, position),
            )

            # Process transactions
            nice_hash_count = 0
            last_nice_hash = None
            max_zero = int(
                cursor.execute(
                    "SELECT value FROM stats WHERE key = 'max_zero'"
                ).fetchone()["value"]
            )
            supply_delta = 0
            utxos_count_delta = 0
            supply_check_delta = 0

            for tx in transactions:
                txid = tx.get("txid")
                reward = tx.get("reward", 0)

                # Process nicehash if it has a reward
                if txid and reward > 0:
                    nice_hash_count += 1
                    last_nice_hash = txid
                    cursor.execute(
                        "INSERT INTO nicehashes (height, txid, reward) VALUES (?, ?, ?)",
                        (height, txid, reward),
                    )
                    zero_count = count_zeros(txid)
                    if zero_count > max_zero:
                        cursor.execute(
                            "UPDATE stats SET value = ? WHERE key = 'max_zero'",
                            (zero_count,),
                        )
                        cursor.execute(
                            "UPDATE stats SET value = ? WHERE key = 'nicest_hash'",
                            (txid,),
                        )
                        max_zero = zero_count
                    supply_delta += reward

                # Process balance operations
                for operation in tx.get("balance_ops", []):
                    op_type = operation.get("type")
                    utxo_id_bytes = operation.get("utxo_id")
                    balance = operation.get("balance", 0)

                    if utxo_id_bytes:
                        if op_type == "add":
                            self.add_balance(utxo_id_bytes, balance)
                            if balance > 0:
                                supply_check_delta += balance
                                utxos_count_delta += 1
                        elif op_type == "remove":
                            self.remove_balance(utxo_id_bytes)
                            if balance > 0:
                                supply_check_delta -= balance
                                utxos_count_delta -= 1

            # Update statistics
            if nice_hash_count > 0:
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = 'nice_hashes_count'",
                    (nice_hash_count,),
                )
            if last_nice_hash is not None:
                cursor.execute(
                    "UPDATE stats SET value = ? WHERE key = 'last_nice_hash'",
                    (last_nice_hash,),
                )
            if supply_delta > 0:
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = 'supply'",
                    (supply_delta,),
                )
            
            if utxos_count_delta != 0:
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = 'utxos_count'",
                    (utxos_count_delta,),
                )
            
            if supply_check_delta != 0:
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = 'supply_check'",
                    (supply_check_delta,),
                )

            # Update last parsed block
            cursor.execute(
                "UPDATE stats SET value = ? WHERE key = 'last_parsed_block'", (height,)
            )

            # Update last indexed block
            self.last_indexed_block = max(height, self.last_indexed_block)

        # Flush tous les buffers à la fin du traitement du bloc
        for shard_id in range(10):
            self._flush_operation_buffer(shard_id)

    def rollback_block(self, height, rollback_operations=None):
        """Rollback a block from the database
        
        Args:
            height (int): Height of the block to roll back
            rollback_operations (list, optional): List of operations to apply to rollback balances
            
        Returns:
            bool: True if the rollback succeeded, False otherwise
        """
        cursor = self.db.cursor()

        # Check if block exists
        block_info = cursor.execute(
            "SELECT hash, file_number, position FROM blocks WHERE height = ?", (height,)
        ).fetchone()

        if not block_info:
            print(f"Cannot rollback: block {height} not found in the database")
            return False

        # Get nicehashes to delete
        nicehashes = cursor.execute(
            "SELECT txid, reward FROM nicehashes WHERE height = ?", (height,)
        ).fetchall()

        with self.db:
            # Delete block from blocks table
            cursor.execute("DELETE FROM blocks WHERE height = ?", (height,))

            # Delete related nicehashes
            cursor.execute("DELETE FROM nicehashes WHERE height = ?", (height,))

            # Update statistics
            if nicehashes:
                nice_hash_count = len(nicehashes)
                total_reward = sum(nh["reward"] for nh in nicehashes)

                # Reduce nicehashes count
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) - ? AS TEXT) WHERE key = 'nice_hashes_count'",
                    (nice_hash_count,),
                )

                # Reduce supply
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) - ? AS TEXT) WHERE key = 'supply'",
                    (total_reward,),
                )

                # Update last nicehash
                prev_nicehash = cursor.execute(
                    "SELECT txid FROM nicehashes ORDER BY height DESC LIMIT 1"
                ).fetchone()
                if prev_nicehash:
                    cursor.execute(
                        "UPDATE stats SET value = ? WHERE key = 'last_nice_hash'",
                        (prev_nicehash["txid"],),
                    )

            # Update last parsed block
            cursor.execute(
                "UPDATE stats SET value = ? WHERE key = 'last_parsed_block'",
                (height - 1,),
            )

            # Update last indexed block
            self.last_indexed_block = height - 1

        # Apply rollback operations to the shards if provided
        if rollback_operations:
            add_count = sum(1 for op in rollback_operations if op[0] == "add")
            remove_count = sum(1 for op in rollback_operations if op[0] == "remove")
            utxos_count_delta = add_count - remove_count
            
            if utxos_count_delta != 0:
                cursor.execute(
                    "UPDATE stats SET value = CAST(CAST(value AS INTEGER) + ? AS TEXT) WHERE key = 'utxos_count'",
                    (utxos_count_delta,),
                )

            self._apply_rollback_operations(rollback_operations)
            print(f"Applied {len(rollback_operations)} rollback operations to shards")

        print(f"Successfully rolled back block {height} from indexes")
        return True

    def _apply_rollback_operations(self, rollback_operations):
        """Apply rollback operations to update shard balances
        
        Args:
            rollback_operations (list): List of operations to apply
        """
        # Track count of operations by type for logging
        add_count = 0
        remove_count = 0
        
        # Préparer des buffers par shard pour les opérations de rollback
        rollback_buffers = {shard_id: [] for shard_id in range(10)}
        
        for op in rollback_operations:
            op_type = op[0]
            
            if op_type == "remove":
                # This was an add operation in the original block, so we remove the balance
                key = op[1]
                utxo_id = key.to_bytes(8, 'little')
                shard_id = get_shard_id(utxo_id)
                
                rollback_buffers[shard_id].append(("remove_balance", (utxo_id,)))
                remove_count += 1
                    
            elif op_type == "add":
                # This was a remove operation in the original block, so we add the balance back
                key, balance = op[1], op[2]
                utxo_id = key.to_bytes(8, 'little')
                shard_id = get_shard_id(utxo_id)
                
                rollback_buffers[shard_id].append(("add_balance", (utxo_id, balance)))
                add_count += 1
        
        # Envoyer les buffers aux shards
        for shard_id, buffer in rollback_buffers.items():
            if buffer and shard_id in self.shard_queues:
                self.shard_queues[shard_id].put(("batch_operations", buffer))
        
        print(f"Rollback balance operations: added {add_count}, removed {remove_count}")


    def get_last_indexed_block(self):
        """Get the height of the last indexed block"""
        return self.last_indexed_block

    def close(self):
        """Close database connections et ressources associées"""
        print("Fermeture de la base de données...")
        shutdown_start = time.time()
        shutdown_timeout = 20  # Timeout global pour la fermeture
        shutdown_deadline = shutdown_start + shutdown_timeout
        
        # Étape 1: Vidage des buffers d'opérations avec timeout
        try:
            print("Vidage des buffers d'opérations...")
            buffer_flush_timeout = min(3, (shutdown_deadline - time.time()) / 2)
            buffer_flush_start = time.time()
            
            for shard_id in range(10):
                try:
                    if time.time() - buffer_flush_start > buffer_flush_timeout:
                        print(f"Timeout du vidage des buffers après {buffer_flush_timeout:.2f}s")
                        break
                        
                    self._flush_operation_buffer(shard_id)
                except Exception as e:
                    print(f"Erreur lors du vidage du buffer {shard_id}: {e}")
        except Exception as e:
            print(f"Erreur générale lors du vidage des buffers: {e}")
        
        # Étape 2: Drainer les queues pour éviter les blocages
        try:
            print("Drainage des queues...")
            for shard_id, queue in list(self.shard_queues.items()):
                try:
                    # Vider la queue rapidement
                    while True:
                        try:
                            queue.get_nowait()
                        except:
                            break
                except Exception as e:
                    print(f"Erreur lors du drainage de la queue du shard {shard_id}: {e}")
        except Exception as e:
            print(f"Erreur générale lors du drainage des queues: {e}")
        
        # Étape 3: Signaler l'arrêt aux processus
        try:
            print("Signalement de l'arrêt aux processus...")
            self.stop_event.set()
            self.result_processor_stop.set()
        except Exception as e:
            print(f"Erreur lors du signalement d'arrêt: {e}")

        # Étape 4: Envoyer des signaux d'arrêt explicites aux queues sans attente
        try:
            print("Envoi des signaux d'arrêt aux queues...")
            for shard_id, queue in list(self.shard_queues.items()):
                try:
                    # Utiliser put_nowait pour éviter tout blocage
                    queue.put_nowait(None)  # Signal to stop
                except Exception as e:
                    print(f"Erreur lors de l'envoi du signal d'arrêt au shard {shard_id}: {e}")
        except Exception as e:
            print(f"Erreur lors de l'envoi des signaux d'arrêt: {e}")

        # Étape 5: Attendre la fin des processus avec une stratégie progressive
        remaining_time = max(0, shutdown_deadline - time.time())
        if remaining_time > 0:
            print(f"Attente de la fin des processus (max {remaining_time:.2f}s)...")
            
            # Phase 1: Join avec timeout court - attente coopérative
            join_timeout = min(2.0, remaining_time / 3)
            join_start = time.time()
            
            remaining_processes = list(self.shard_processes.items())
            for shard_id, process in remaining_processes[:]:
                # Vérifier si on a encore du temps
                if time.time() - join_start > join_timeout:
                    break
                    
                if process and process.is_alive():
                    try:
                        # Timeout court par processus
                        process_timeout = min(0.5, (join_timeout - (time.time() - join_start)))
                        process.join(process_timeout)
                        if not process.is_alive():
                            remaining_processes.remove((shard_id, process))
                            print(f"Processus shard {shard_id} terminé normalement")
                    except Exception as e:
                        print(f"Erreur lors de l'attente du processus shard {shard_id}: {e}")
            
            # Phase 2: Terminate - arrêt semi-forcé
            if remaining_processes and time.time() < shutdown_deadline:
                terminate_timeout = min(2.0, (shutdown_deadline - time.time()) / 2)
                terminate_start = time.time()
                
                print(f"Terminaison forcée de {len(remaining_processes)} processus...")
                for shard_id, process in remaining_processes[:]:
                    # Vérifier si on a encore du temps
                    if time.time() - terminate_start > terminate_timeout:
                        break
                        
                    if process and process.is_alive():
                        try:
                            process.terminate()
                            # Attente courte après terminate
                            process.join(0.3)
                            if not process.is_alive():
                                remaining_processes.remove((shard_id, process))
                                print(f"Processus shard {shard_id} terminé par signal")
                        except Exception as e:
                            print(f"Erreur lors de la terminaison du processus {shard_id}: {e}")
            
            # Phase 3: Kill - arrêt forcé pour les processus tenaces
            if remaining_processes and time.time() < shutdown_deadline:
                print(f"Kill de {len(remaining_processes)} processus restants...")
                for shard_id, process in remaining_processes:
                    if process and process.is_alive():
                        try:
                            if hasattr(process, "kill"):  # Python 3.7+
                                process.kill()
                            else:
                                try:
                                    import os
                                    import signal
                                    os.kill(process.pid, signal.SIGKILL)
                                except:
                                    pass
                                
                            # Juste une petite attente pour voir si ça a fonctionné
                            time.sleep(0.1)
                            if process.is_alive():
                                print(f"⚠️ Le processus {shard_id} n'a pas pu être tué!")
                            else:
                                print(f"Processus {shard_id} tué")
                        except Exception as e:
                            print(f"Impossible de killer le processus {shard_id}: {e}")
        
        # Étape 6: Attendre le processeur de résultats sans bloquer plus de 0.5s
        if time.time() < shutdown_deadline:
            print("Attente de la fin du processeur de résultats...")
            if self.result_processor and self.result_processor.is_alive():
                try:
                    self.result_processor.join(min(0.5, shutdown_deadline - time.time()))
                except Exception as e:
                    print(f"Erreur lors de l'attente du processeur de résultats: {e}")
        
        # Étape 7: Fermer la connexion principale à la base de données
        print("Fermeture de la connexion principale à la base de données...")
        if self.db:
            try:
                # On utilise une autre thread avec timeout pour éviter un blocage
                db_close_success = [False]  # Liste mutable pour permettre la modification dans la thread
                
                def close_db():
                    try:
                        self.db.close()
                        db_close_success[0] = True
                    except Exception as e:
                        print(f"Erreur dans la thread de fermeture DB: {e}")
                
                db_close_thread = threading.Thread(target=close_db)
                db_close_thread.daemon = True
                db_close_thread.start()
                
                # Attendre la fin avec timeout
                db_close_timeout = min(1.0, max(0, shutdown_deadline - time.time()))
                db_close_thread.join(db_close_timeout)
                
                if db_close_thread.is_alive():
                    print("⚠️ La fermeture de la connexion principale est bloquée, on continue...")
                elif db_close_success[0]:
                    print("Base de données principale fermée avec succès")
                else:
                    print("⚠️ Échec de la fermeture de la base de données")
                    
            except Exception as e:
                print(f"Erreur lors de la fermeture de la base de données: {e}")

        # Afficher le temps total de fermeture
        shutdown_time = time.time() - shutdown_start
        print(f"Base de données fermée en {shutdown_time:.2f} secondes")
        
        # Si on dépasse le timeout global, on le signale
        if shutdown_time > shutdown_timeout:
            print(f"⚠️ La fermeture a dépassé le timeout de {shutdown_timeout}s")