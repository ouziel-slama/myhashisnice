import time
import json
import decimal
import sys
import time
import binascii

import apsw
import apsw.bestpractice
import requests

from lib import CONFIG, Config, connect_db, RSFetcher, UTXOSet
from nicefetcher import utils

D = decimal.Decimal

from nicefetcher import indexer


class BitcoindRPCError(Exception):
    pass


def create_indexes(cursor, table, indexes, unique=False):
    for index in indexes:
        field_names = [field.split(" ")[0] for field in index]
        index_name = f"{table}_{'_'.join(field_names)}_idx"
        fields = ", ".join(index)
        unique_clause = "UNIQUE" if unique else ""
        query = f"""
            CREATE {unique_clause} INDEX IF NOT EXISTS {index_name} ON {table} ({fields})
        """
        cursor.execute(query)


def optimize_db(db):
    start_time = time.time()
    print("Applying best practices to database...")
    apsw.bestpractice.apply(apsw.bestpractice.recommended) 
    cursor = db.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    #print("Vacuuming database...")
    #cursor.execute("VACUUM")
    #print("Analyzing database...")
    #cursor.execute("ANALYZE")
    elapsed_time = time.time() - start_time
    print(f"Database optimized in {elapsed_time:.3f}s")


def init_db(db):
    cursor = db.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS txids (
            txid BLOB UNIQUE,
            height INTEGER,
            position INTEGER,
            zero_count INTEGER
        )
        """
    )
    create_indexes(
        cursor,
        "txids",
        [
            ["height"],
            ["zero_count"],
        ],
    )

    for i in range(10):
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS utxos_{i} (
                height INTEGER,
                position INTEGER,
                vout INTEGER,
                utxo_id BLOB,
                source_utxo_id BLOB,
                balance INTEGER,
                address_hash BLOB,
                reward BOOLEAN
            )
            """
        )
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blocks (
            height INTEGER,
            hash TEXT,
            movement_count INTEGER
        )
        """
    )
    create_indexes(
        cursor,
        "blocks",
        [
            ["height"],
            ["hash"],
            ["movement_count"],
        ],
    )

    select_utxos = []
    for i in range(10):
        select_utxos.append(f"SELECT * FROM utxos_{i}")
    select_utxos = " UNION ALL ".join(select_utxos)
    cursor.execute(f"CREATE VIEW IF NOT EXISTS all_utxos AS {select_utxos}")


def create_utxos_indexes(db):
    cursor = db.cursor()
    for i in range(10):
        start_time = time.time()
        print(f"Creating indexes for utxos_{i}...")
        create_indexes(
            cursor,
            f"utxos_{i}",
            [
                ["height", "position", "vout"],
                ["utxo_id"],
                ["source_utxo_id"],
                ["balance"],
                ["address_hash"],
                ["reward"],
            ],
        )
        elapsed_time = time.time() - start_time
        print(f"Indexes created in {elapsed_time:.3f}s")


def generate_movements(
    height, tx, position, source_utxo_id, quantity, output_values, zero_count
):
    movements = []
    distribution = calculate_distribution(quantity, output_values)
    # gather movements
    for out in tx["vout"]:
        if not out["is_op_return"] and distribution:
            movements.append(
                {
                    "height": height,
                    "txid": tx["tx_id"],
                    "address_hash": out["address_hash"],
                    "destination": out["n"],
                    "quantity": distribution.pop(0),
                    "zero_count": zero_count,
                    "position": position,
                    "utxo_id": out["utxo_id"],
                    "source_utxo_id": source_utxo_id,
                }
            )
    return movements

def insert_movement(db, movement):
    # insert destination utxo
    binding = {
        "height": movement["height"],
        "position": movement["position"],
        "vout": movement["destination"],
        "utxo_id": movement["utxo_id"],
        "source_utxo_id": movement["source_utxo_id"],
        "balance": movement["quantity"],
        "address_hash": movement["address_hash"],
        "reward": movement["source_utxo_id"] == "reward",
    }
    table_name = f"utxos_{movement['height'] % 10}"
    db.execute(
        f"""
        INSERT INTO {table_name} (height, position, vout, utxo_id, source_utxo_id, balance, address_hash, reward)
        VALUES (:height, :position, :vout, :utxo_id, :source_utxo_id, :balance, :address_hash, :reward)
        """,
        binding
    )

    #UTXOSet().add(movement["utxo_id"], movement["quantity"])

    # insert movements
    if movement["source_utxo_id"] == "reward":
        nice_hash = utils.inverse_hash(binascii.hexlify(movement["txid"]).decode())
        print(f'{movement["quantity"]} rewarded to {nice_hash}:{movement["destination"]}')


def save_movements(db, movements):
    for movement in movements:
        insert_movement(db, movement)


def get_balance(db, utxo_id):
    for i in range(10):
        sql = f"SELECT balance FROM utxos_{i} WHERE utxo_id = :utxo_id"
        utxo = db.execute(sql, {"utxo_id": utxo_id}).fetchone()
        if utxo:
            return utxo["balance"]
    return 0


def rpc_call(method, params):
    try:
        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 0,
        }
        response = requests.post(
            Config()["BACKEND_URL"],
            data=json.dumps(payload),
            headers={"content-type": "application/json"},
            verify=(not Config()["BACKEND_SSL_NO_VERIFY"]),
            timeout=Config()["REQUESTS_TIMEOUT"],
        ).json()
        if "error" in response and response["error"]:
            print(response)
            raise BitcoindRPCError(f"Error calling {method}: {response['error']}")
        return response["result"]
    except (
        requests.exceptions.RequestException,
        json.decoder.JSONDecodeError,
        KeyError,
    ) as e:
        raise BitcoindRPCError(f"Error calling {method}: {str(e)}") from e


def get_decoded_block(height, verbosity=2):
    block_hash = rpc_call("getblockhash", [height])
    return rpc_call("getblock", [block_hash, verbosity])



def calculate_reward(zero_count, max_zero_count):
    if zero_count < Config()["MIN_ZERO_COUNT"]:
        return 0
    reward = D(Config()["MAX_REWARD"]) / D(16 ** (max_zero_count - zero_count))
    return int(reward)


def calculate_distribution(value, output_values):
    total_output = sum(output_values)
    if total_output == 0:
        return [value] + [0] * (len(output_values) - 1)

    distribution = [
        int(D(value) * (D(output_value) / D(total_output)))
        for output_value in output_values
    ]
    total_distributed = sum(distribution)

    if total_distributed < value:
        distribution[0] += value - total_distributed

    return distribution


def process_movements(db, block):
    height = block["height"]
    movement_count = 0

    for position, tx in enumerate(block["tx"]):
        #print("Processing tx", i, "of", count, end="\r")
        # exclude coinbase
        if "coinbase" in tx["vin"][0]:
            continue

        # exclude op_return
        output_values = [out["value"] for out in tx["vout"] if not out["is_op_return"]]
        if len(output_values) < 1:
            continue
        # don't distribute to the last output if more than one
        if len(output_values) > 1:
            output_values.pop()

        # reward movements
        zero_count = tx["zero_count"]
        reward = 0
        if zero_count >= Config()["MIN_ZERO_COUNT"]:
            reward = calculate_reward(zero_count, block["max_zero_count"])

        tx_movements = 0
        if reward > 0:
            movements = generate_movements(
                height,
                tx,
                position,
                "reward",
                reward,
                output_values,
                zero_count
            )
            count = len(movements)
            movement_count += count
            tx_movements += count
            save_movements(db, movements)

        # send movements
        for vin in tx["vin"]:
            #balance = UTXOSet().get_balance(vin['utxo_id'])
            balance = get_balance(db, vin['utxo_id'])
            if balance > 0:
                movements = generate_movements(
                    height,
                    tx,
                    position,
                    vin['utxo_id'],
                    balance,
                    output_values,
                    zero_count
                )
                count = len(movements)
                movement_count += count
                tx_movements += count
                save_movements(db, movements)
                #UTXOSet().remove(vin['utxo_id'])

        if tx_movements > 0:
            binding = {
                "txid": tx["tx_id"],
                "height": height,
                "position": position,
                "zero_count": zero_count,
            }
            db.execute(
                """
                INSERT INTO txids (txid, height, position, zero_count)
                VALUES (:txid, :height, :position, :zero_count)
                """, 
                binding
            )
    return movement_count



def get_parsed_block(db, height):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM blocks WHERE height = ?", (height,))
    return cursor.fetchone()


def get_last_parsed_block_height(db):
    cursor = db.cursor()
    cursor.execute("SELECT height FROM blocks ORDER BY height DESC LIMIT 1")
    last_block = cursor.fetchone()
    if last_block:
        return last_block["height"]
    return Config()["START_HEIGHT"]


def rollback(db, height):
    cursor = db.cursor()
    cursor.execute("DELETE FROM blocks WHERE height > ?", (height,))
    cursor.execute("DELETE FROM txids WHERE height > ?", (height,))
    for i in range(10):
        cursor.execute(f"DELETE FROM utxos_{i} WHERE height > ?", (height,))


def check_reorg(db, decoded_block):
    previous_hash = decoded_block["hash_prev"]
    sql = "SELECT height FROM blocks WHERE hash = ?"
    previous_block = db.execute(sql, (previous_hash,)).fetchone()
    if previous_block is None:
        raise Exception(f"Previous block not found: {previous_hash}")
    if previous_block["height"] != decoded_block["height"] - 1:
        print(f"Reorg detected from block {previous_block['height']}. Rolling back...")
        rollback(db, previous_block["height"])


def parse_block(db, decoded_block):
    start_time = time.time()
    movement_count = process_movements(db, decoded_block)
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO blocks (height, hash, movement_count) VALUES (?, ?, ?)
    """,
        (decoded_block["height"], decoded_block["block_hash"], movement_count),
    )
    elapsed_time = time.time() - start_time
    message = f"Block {decoded_block['height']} ({elapsed_time:.3f}s)"
    print(message)


def catch_up(db):
    block_count = rpc_call("getblockcount", [])
    target = block_count - 6
    last_parsed_block_height = get_last_parsed_block_height(db)
    if last_parsed_block_height >= target:
        return
    print(f"Catching up to block {target}")
    try:
        fetcher = RSFetcher(last_parsed_block_height + 1)
        while True:
            decoded_block = fetcher.get_next_block()
            if decoded_block is None:
                break
            with db:
                parse_block(db, decoded_block)
                if decoded_block["height"] == target:
                    break
    finally:
        fetcher.stop()


def deserialize_block(block_hex, block_index):
    deserializer = indexer.Deserializer(
        {
            "rpc_address": "",
            "rpc_user": "",
            "rpc_password": "",
            "network": Config().network_name,
            "db_dir": "",
            "log_file": "",
            "prefix": b"prefix",
        }
    )
    decoded_block = deserializer.parse_block(block_hex, block_index)
    decoded_block["tx"] = decoded_block.pop("transactions")
    return decoded_block


def follow(db):
    block_count = rpc_call("getblockcount", [])
    last_parsed_block_height = get_last_parsed_block_height(db)
    if last_parsed_block_height >= block_count:
        return
    block_hash = rpc_call("getblockhash", [last_parsed_block_height + 1])
    raw_block = rpc_call("getblock", [block_hash, 0])
    decoded_block = deserialize_block(raw_block, last_parsed_block_height + 1)
    check_reorg(db, decoded_block)
    with db:
        parse_block(db, decoded_block)


def start_parser(network_name):
    print(f"Starting parser for {network_name}")
    Config().set_network(network_name)
    try:
        db = connect_db()
        init_db(db)
        optimize_db(db)
        # UTXOSet()
        catch_up(db)
        print("Waiting for new block...")
        while True:
            follow(db)
            time.sleep(10)

        # create_utxos_indexes(db)
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        #UTXOSet().close()
        print("Closing database...")
        db.close()
        print("Shutdown complete")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        network_name = "regtest"
    else:
        network_name = sys.argv[1]
    start_parser(network_name)
