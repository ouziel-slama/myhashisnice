import binascii
import requests
from contextlib import contextmanager
from config import SingletonMeta, Config
from nicefetcher import utils
from mhinindexes import apsw_connect

class APSWConnectionPool(metaclass=SingletonMeta):
    def __init__(self, db_file):
        self.connections = []
        self.db_file = db_file
        self.closed = False
        self.pool_size = 10

    @contextmanager
    def connection(self):
        if self.connections:
            # Reusing connection
            db = self.connections.pop(0)
        else:
            # New db connection
            db = apsw_connect(self.db_file)
        try:
            yield db
        finally:
            if self.closed:
                db.close()
            elif len(self.connections) < self.pool_size:
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


def utxo_to_utxo_id(txid, n):
    txid = utils.inverse_hash(txid)
    txid = binascii.unhexlify(txid)
    return bytes(utils.pack_utxo(txid, n))


class MhinQueries:
    def __init__(self, db_file=None):
        if db_file is None:
            db_file = f"{Config()['BALANCES_STORE']}/mhin_indexes.db"
        self.pool = APSWConnectionPool(db_file)
        
        # Cache for statistics
        self._stats_cache = None
        self._stats_cache_block_height = 0
        self._latest_nicehashes_cache = None
        self._latest_nicehashes_cache_block_height = 0
    
    def get_balance_by_address(self, address):
        """
        Fetch UTXOs for an address using mempool.space API, 
        convert them to utxo_id, and sum up their balances.
        
        Args:
            address (str): Bitcoin address
        
        Returns:
            dict: Balance information including total balance and UTXOs
        """
        # Get UTXOs from mempool.space API
        try:
            api_url = f"https://mempool.space/api/address/{address}/utxo"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            utxos = response.json()
        except requests.RequestException as e:
            print(f"Error fetching UTXOs for address {address}: {e}")
            return {"total_balance": 0, "utxos": []}
        
        total_balance = 0
        utxo_details = []
        
        with self.pool.connection() as db:
            cursor = db.cursor()
            
            for utxo in utxos:
                txid = utxo.get('txid')
                vout = utxo.get('vout')
                
                if txid and vout is not None:
                    # Convert to utxo_id
                    utxo_id = utxo_to_utxo_id(txid, vout)
                    
                    # Query the balance
                    row = cursor.execute(
                        "SELECT balance FROM balances WHERE utxo_id = ?", 
                        (utxo_id,)
                    ).fetchone()
                    
                    balance = 0
                    if row:
                        balance = row['balance']
                        total_balance += balance
                    
                    utxo_details.append({
                        "txid": txid,
                        "vout": vout,
                        "balance": balance
                    })
        
        return {
            "address": address,
            "total_balance": total_balance,
            "utxos": utxo_details
        }

    def get_latest_nicehashes(self, limit=50):
        """
        Return the most recent nicehashes with their rewards and block heights.
        
        Args:
            limit (int): Number of nicehashes to return
        
        Returns:
            list: List of dictionaries containing nicehash information
        """
        # Check if we can use cached result
        with self.pool.connection() as db:
            cursor = db.cursor()
            max_height = cursor.execute("SELECT MAX(height) as max_height FROM blocks").fetchone()
            current_max_height = max_height['max_height'] if max_height and max_height['max_height'] is not None else 0
            
            # Use cache if available and still valid
            if (self._latest_nicehashes_cache is not None and 
                self._latest_nicehashes_cache_block_height == current_max_height):
                return self._latest_nicehashes_cache
            
            # Query the latest nicehashes
            nicehashes = []
            for row in cursor.execute(
                """
                SELECT height, txid, reward 
                FROM nicehashes 
                ORDER BY height DESC 
                LIMIT ?
                """, 
                (limit,)
            ):
                nicehashes.append({
                    "height": row['height'],
                    "txid": row['txid'],
                    "reward": row['reward']
                })
            
            # Update cache
            self._latest_nicehashes_cache = nicehashes
            self._latest_nicehashes_cache_block_height = current_max_height
            
            return nicehashes

    def get_stats(self):
        """
        Return various statistics about the system.
        
        Returns:
            dict: Statistics including MHIN supply, nice hashes count, etc.
        """
        with self.pool.connection() as db:
            cursor = db.cursor()
            
            # Get current max block height for cache validation
            max_height = cursor.execute("SELECT MAX(height) as max_height FROM blocks").fetchone()
            current_max_height = max_height['max_height'] if max_height and max_height['max_height'] is not None else 0
            
            # Use cache if available and still valid
            if self._stats_cache is not None and self._stats_cache_block_height == current_max_height:
                return self._stats_cache
            
            # Calculate MHIN Supply (sum of all balances)
            mhin_supply = cursor.execute("SELECT SUM(balance) as total FROM balances").fetchone()
            mhin_supply = mhin_supply['total'] if mhin_supply and mhin_supply['total'] is not None else 0
            
            # Count of nice hashes
            nice_hashes_count = cursor.execute("SELECT COUNT(*) as count FROM nicehashes").fetchone()
            nice_hashes_count = nice_hashes_count['count'] if nice_hashes_count else 0
            
            # Nicest hash (with most leading zeros)
            nicest_hash = cursor.execute(
                """
                SELECT txid, reward, height, 
                       LENGTH(txid) - LENGTH(LTRIM(txid, '0')) AS leading_zeros
                FROM nicehashes
                ORDER BY leading_zeros DESC, height DESC
                LIMIT 1
                """
            ).fetchone()
            
            # Last nice hash
            last_nice_hash = cursor.execute(
                "SELECT txid, reward, height FROM nicehashes ORDER BY height DESC LIMIT 1"
            ).fetchone()
            
            # First nice hash
            first_nice_hash = cursor.execute(
                "SELECT txid, reward, height FROM nicehashes ORDER BY height ASC LIMIT 1"
            ).fetchone()
            
            # UTXOs count
            utxos_count = cursor.execute("SELECT COUNT(*) as count FROM balances").fetchone()
            utxos_count = utxos_count['count'] if utxos_count else 0
            
            # Prepare and format the results
            stats = {
                "supply": mhin_supply,
                "nice_hashes_count": nice_hashes_count,
                "nicest_hash": nicest_hash['txid'],
                "last_nice_hash": last_nice_hash['txid'],
                "first_nice_hash": first_nice_hash['txid'],
                "utxos_count": utxos_count
            }
            
            # Update cache
            self._stats_cache = stats
            self._stats_cache_block_height = current_max_height
            
            return stats