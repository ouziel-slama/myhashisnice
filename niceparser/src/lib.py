import json
import decimal

import apsw
import apsw.bestpractice

from contextlib import contextmanager

apsw.bestpractice.apply(apsw.bestpractice.recommended)  # includes WAL mode

DB_CONNECTION_POOL_SIZE = 10

CONFIG = {
    "mainnet": {
        "BACKEND_URL": "http://rpc:rpc@localhost:8332",
        "BACKEND_SSL_NO_VERIFY": True,
        "REQUESTS_TIMEOUT": 10,
        "DATABASE_FILE": "niceparser.db",
        "MIN_ZERO_COUNT": 6,
        "MAX_REWARD": 4096 * 1e8,
        "UNIT": 1e8,
    },
    "regtest": {
        "BACKEND_URL": "http://rpc:rpc@localhost:18443",
        "BACKEND_SSL_NO_VERIFY": True,
        "REQUESTS_TIMEOUT": 10,
        "DATABASE_FILE": "niceparser.regtest.db",
        "MIN_ZERO_COUNT": 2,
        "MAX_REWARD": 4096 * 1e8,
        "UNIT": 1e8,
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
    cursor.execute("PRAGMA synchronous = normal")
    cursor.execute("PRAGMA journal_size_limit = 6144000")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA defer_foreign_keys = ON")
    db.setrowtrace(rowtracer)
    cursor.close()
    return db


def get_balance(db, utxo):
    sql = """
        SELECT * FROM balances WHERE utxo = :utxo
    """
    cursor = db.cursor()
    cursor.execute(sql, {"utxo": utxo})
    return cursor.fetchone()


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