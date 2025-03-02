import os
from pathlib import Path

home = Path.home()
DATA_DIR = str(home / "data")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

CONFIG = {
    "mainnet": {
        "DATA_DIR": DATA_DIR,
        "BACKEND_URL": "http://rpc:rpc@localhost:8332",
        "BACKEND_SSL_NO_VERIFY": True,
        "REQUESTS_TIMEOUT": 10,
        "FETCHER_DB": os.path.join(DATA_DIR, "fetcherdb"),
        "BALANCES_STORE": os.path.join(DATA_DIR, "balances"),
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
        "BALANCES_STORE": os.path.join(DATA_DIR, "balances.regtest"),
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