#CFLAGS="-I/opt/homebrew/Cellar/leveldb/1.23_2/include -fno-rtti" LDFLAGS="-L/opt/homebrew/Cellar/leveldb/1.23_2/lib"  pip3 install plyvel --no-cache-dir
import json
import os
import decimal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
from collections.abc import MutableMapping
import struct
import time
from collections import defaultdict
import threading


import apsw
#import apsw.bestpractice

from nicefetcher import indexer, utils

from contextlib import contextmanager

#apsw.bestpractice.apply(apsw.bestpractice.recommended)  # includes WAL mode

DB_CONNECTION_POOL_SIZE = 10

home = Path.home()
#DATA_DIR = "/Volumes/CORSAIR/.niceparser"
DATA_DIR = str(home / ".niceparser")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

CONFIG = {
    "mainnet": {
        "DATA_DIR": DATA_DIR,
        "BACKEND_URL": "http://rpc:rpc@localhost:8332",
        "BACKEND_SSL_NO_VERIFY": True,
        "REQUESTS_TIMEOUT": 10,
        "DATABASE_FILE": os.path.join(DATA_DIR, "niceparser.db"),
        "FETCHER_DB": os.path.join(DATA_DIR, "fetcherdb"),
        "UTXOSET_DB": os.path.join(DATA_DIR, "utxo_set.db"),
        "MIN_ZERO_COUNT": 6,
        "MAX_REWARD": 4096 * 1e8,
        "UNIT": 1e8,
        "START_HEIGHT": 232500,
    },
    "regtest": {
        "DATA_DIR": DATA_DIR,
        "BACKEND_URL": "http://rpc:rpc@localhost:18443",
        "BACKEND_SSL_NO_VERIFY": True,
        "REQUESTS_TIMEOUT": 10,
        "DATABASE_FILE": os.path.join(DATA_DIR, "niceparser.regtest.db"),
        "FETCHER_DB": os.path.join(DATA_DIR, "fetcherdb.regtest"),
        "UTXOSET_DB": os.path.join(DATA_DIR, "utxo_set.regtest.db"),
        "MIN_ZERO_COUNT": 2,
        "MAX_REWARD": 4096 * 1e8,
        "UNIT": 1e8,
        "START_HEIGHT": 119,
    },
}


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Config(metaclass=SingletonMeta):
    def __init__(self):
        self.config = {}

    def set_network(self, network_name):
        self.network_name = network_name
        self.config = CONFIG[network_name]

    def __getitem__(self, key):
        return self.config[key]


class RSFetcher():
    def __init__(self, start_height=0):
        self.fetcher = indexer.Indexer({
            "rpc_address": Config()["BACKEND_URL"],
            "rpc_user": "rpc",
            "rpc_password": "rpc",
            "db_dir": Config()["FETCHER_DB"],
            "start_height": start_height,
            "log_file": "/tmp/fetcher.log",
            "only_write_in_reorg_window": True,
        })
        self.fetcher.start()
        self.prefeteched_block = queue.Queue(maxsize=10)
        self.prefetched_count = 0
        self.stopped_event = threading.Event()
        self.executors = []
        for _i in range(2):
            executor = threading.Thread(target=self.prefetch_block, args=(self.stopped_event,))
            executor.daemon = True
            executor.start()
            self.executors.append(executor)

    def get_next_block(self):
        while not self.stopped_event.is_set():
            try:
                return self.prefeteched_block.get(timeout=1)
            except queue.Empty:
                self.stopped_event.wait(timeout=0.1)
                continue
        return None  # Return None only if stopped
    
    def prefetch_block(self, stopped_event):
        while not stopped_event.is_set():
            if self.prefeteched_block.qsize() > 10:
                # Add sleep to prevent CPU spinning
                stopped_event.wait(timeout=0.1)
                continue
                
            block = self.fetcher.get_block_non_blocking()
            if block is None:
                # Add sleep when no block is available
                stopped_event.wait(timeout=0.1)
                continue
                
            block["tx"] = block.pop("transactions")
            while not stopped_event.is_set():
                try:
                    self.prefeteched_block.put(block, timeout=1)
                    break
                except queue.Full:
                    # Using event.wait instead of time.sleep
                    stopped_event.wait(timeout=0.1)
    
    def stop(self):
        self.stopped_event.set()
        try:
            self.fetcher.stop()
        except Exception as e:
            pass



def rowtracer(cursor, sql):
    """Converts fetched SQL data into dict-style"""
    return {
        name: (bool(value) if str(field_type) == "BOOL" else value)
        for (name, field_type), value in zip(cursor.getdescription(), sql)
    }


def connect_db():
    db = apsw.Connection(Config()["DATABASE_FILE"])
    cursor = db.cursor()
    cursor.execute("PRAGMA case_sensitive_like = ON")
    cursor.execute("PRAGMA auto_vacuum = 1")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_size_limit = 6144000")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA defer_foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    db.setrowtrace(rowtracer)
    cursor.close()
    return db


class ApiJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return "{0:.8f}".format(o)
        if isinstance(o, bytes):
            return o.hex()
        if callable(o):
            return o.__name__
        if hasattr(o, "__class__"):
            return str(o)
        return super().default(o)


def to_json(obj, indent=None):
    return json.dumps(obj, cls=ApiJsonEncoder, indent=indent)


class APSWConnectionPool(metaclass=SingletonMeta):
    def __init__(self, db_file):
        self.connections = []
        self.db_file = db_file
        self.closed = False

    @contextmanager
    def connection(self):
        if self.connections:
            # Reusing connection
            db = self.connections.pop(0)
        else:
            # New db connection
            db = connect_db()
        try:
            yield db
        finally:
            if self.closed:
                db.close()
            elif len(self.connections) < DB_CONNECTION_POOL_SIZE:
                # Add connection to pool
                self.connections.append(db)
            else:
                # Too much connections in the pool: closing connection
                db.close()

    def close(self):
        self.closed = True
        while len(self.connections) > 0:
            db = self.connections.pop()
            db.close()


class DoubleHashDict:
    def __init__(self, size_hint=1024):
        self.l1_size = size_hint
        # Premier niveau de buckets
        self.l1_buckets = [None] * self.l1_size
    
    def _get_bucket_indexes(self, key: bytes) -> tuple[int, int]:
        # Premier niveau: 4 premiers bytes
        l1_idx = int.from_bytes(key[:4], 'little') % self.l1_size
        # Second niveau: 4 derniers bytes
        l2_idx = int.from_bytes(key[4:], 'little') % self.l1_size
        return l1_idx, l2_idx
    
    def add(self, key: bytes, value: int):
        if len(key) != 8:
            raise ValueError("Key must be exactly 8 bytes")
            
        l1_idx, l2_idx = self._get_bucket_indexes(key)
        
        # Initialiser le bucket de niveau 1 si nécessaire
        if self.l1_buckets[l1_idx] is None:
            self.l1_buckets[l1_idx] = [dict() for _ in range(self.l1_size)]
            
        # Stocker dans le bucket de niveau 2
        self.l1_buckets[l1_idx][l2_idx][key] = value
    
    def pop(self, key: bytes, default: int = 0) -> int:
        if len(key) != 8:
            raise ValueError("Key must be exactly 8 bytes")
            
        l1_idx, l2_idx = self._get_bucket_indexes(key)
        
        # Si le bucket de niveau 1 n'existe pas, la clé n'existe pas
        if self.l1_buckets[l1_idx] is None:
            return default
            
        # Retourner du bucket de niveau 2
        return self.l1_buckets[l1_idx][l2_idx].pop(key, default)


class UTXOSet(metaclass=SingletonMeta):
    def __init__(self):
        self.init()

    def init(self):
        self.utxo_set = DoubleHashDict()
        self.backup_file_path = Config()["UTXOSET_DB"]
        if os.path.exists(self.backup_file_path):
            self.load_backup()
        self.backup_file = open(self.backup_file_path, "ab")

    def add(self, utxo_id, value):
        self.utxo_set.add(utxo_id, value)
        packed = struct.pack('<c8sQ', b"A", utxo_id, value)
        self.backup_file.write(packed)

    def remove(self, utxo_id):
        packed = struct.pack('<c8s', b"R", utxo_id)
        self.backup_file.write(packed)

    def get_balance(self, utxo_id):
        return self.utxo_set.pop(utxo_id, 0)
    
    def load_backup(self):
        print("Loading UTXO backup...")
        start = time.time()
        i = 0
        with open(self.backup_file_path, "rb") as f:
            while True:
                prefixed_utxo_id = f.read(9)
                length = len(prefixed_utxo_id)
                
                if not prefixed_utxo_id:
                    break
                action, utxo_id = struct.unpack('<c8s', prefixed_utxo_id)
                if action == b"A":
                    value = struct.unpack('<Q', f.read(8))[0]
                    self.utxo_set.add(utxo_id, value)
                else:
                    self.utxo_set.pop(utxo_id, None)
                i += 1
                if i % 10000 == 0:
                    print(f"Loaded {i} UTXOs", end="\r")
        elapsed = time.time() - start
        print(f"UTXO backup loaded in {elapsed:.3f} seconds")

    def close(self):
        print("Closing UTXO backup...")
        self.backup_file.flush()
        self.backup_file.close()
