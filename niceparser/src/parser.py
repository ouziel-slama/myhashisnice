import time
import json
import decimal
import sys

import requests

from lib import CONFIG, Config, connect_db, get_balance

D = decimal.Decimal

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


def init_db(db):
    curosr = db.cursor()
    curosr.execute(
        """
        CREATE TABLE IF NOT EXISTS movements (
            height INTEGER,
            txid TEXT,
            source TEXT,
            destination INTEGER,
            quantity INTEGER,
            destination_address TEXT,
            source_address TEXT
        )
        """
    )
    create_indexes(curosr, "movements", [
        ["height"],
        ["txid"],
        ["source"],
        ["quantity"],
        ["destination_address"],
        ["source_address"],
    ])
    curosr.execute(
        """
        CREATE TABLE IF NOT EXISTS balances (
            utxo TEXT PRIMARY KEY,
            address TEXT,
            quantity INTEGER
        )
        """
    )
    create_indexes(curosr, "balances", [
        ["address"],
        ["quantity"],
    ])
    curosr.execute(
        """
        CREATE TABLE IF NOT EXISTS blocks (
            height INTEGER,
            hash TEXT,
            movement_count INTEGER
        )
        """
    )
    create_indexes(curosr, "blocks", [
        ["height"],
        ["hash"],
        ["movement_count"],
    ])


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
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError, KeyError) as e:
        raise BitcoindRPCError(f"Error calling {method}: {str(e)}") from e


def get_zero_count(txid):
    return len(txid) - len(txid.lstrip("0"))


def get_decoded_block(height):
    block_hash = rpc_call("getblockhash", [height])
    return rpc_call("getblock", [block_hash, 2])


def get_block_max_zero_count(height):
    block = get_decoded_block(height)
    max_zero_count = 0
    for tx in block['tx']:
        zero_count = get_zero_count(tx['txid'])
        if zero_count > max_zero_count:
            max_zero_count = zero_count
    return max_zero_count


def calculate_reward(zero_count, max_zero_count):
    if zero_count < Config()["MIN_ZERO_COUNT"]:
        return 0
    reward = D(Config()["MAX_REWARD"]) / D(16 ** (max_zero_count - zero_count))
    return int(reward)


def calculate_distribution(value, output_values):
    total_output = sum(output_values)
    if total_output == 0:
        return [value] + [0] * (len(output_values) - 1)

    distribution = [int(D(value) * (D(output_value) / D(total_output))) for output_value in output_values]
    total_distributed = sum(distribution)

    if total_distributed < value:
        distribution[0] += value - total_distributed

    return distribution


def generate_movements(height, tx, source, quantity, output_values, source_address=None):
    movements = []
    distribution = calculate_distribution(quantity, output_values)
    # gather movements
    for out in tx['vout']:
        if out["value"] > 0 and distribution:
            movements.append({
                "height": height,
                "txid": tx["txid"],
                "source": source,
                "address": out["scriptPubKey"]["address"],
                "destination": out["n"],
                "utxo": f"{tx['txid']}:{out['n']}",
                "quantity": distribution.pop(0),
                "source_address": source_address,
            })
    return movements


def process_movements(db, block):
    height = block['height']
    movement_count = 0
    for tx in block['tx']:
        # exclude coinbase
        if "coinbase" in tx["vin"][0]:
            continue

        # exclude op_return
        output_values = [out["value"] for out in tx['vout'] if out["value"] > 0]
        if len(output_values) < 1:
            continue
        # don't distribute to the last output if more than one
        if len(output_values) > 1:
            output_values.pop()

        # reward movements
        zero_count = get_zero_count(tx['txid'])
        reward = calculate_reward(zero_count, get_block_max_zero_count(height)) 
        if reward > 0:
            movements = generate_movements(height, tx, "reward", reward, output_values)
            movement_count += len(movements)
            save_movements(db, movements)

        # send movements
        for vin in tx['vin']:
            utxo = f"{vin['txid']}:{vin['vout']}"
            balance = get_balance(db, utxo)
            if balance is not None and balance["quantity"] > 0:
                movements = generate_movements(height, tx, utxo, balance["quantity"], output_values, balance["address"])
                movement_count += len(movements)
                save_movements(db, movements)
    return movement_count


def save_movements(db, movements):
    for movement in movements:
        print(f"Block {movement['height']}: {movement['quantity']} from {movement['source']} to {movement['utxo']} ({movement['address']})")
    # insert movements into database
    sql = """
        INSERT INTO movements (height, txid, source, destination, quantity, destination_address, source_address)
        VALUES (:height, :txid, :source, :destination, :quantity, :address, :source_address)
    """
    cursor = db.cursor()
    cursor.executemany(sql, movements)
    
    for movement in movements:
        # update destination balance
        sql = """
            INSERT INTO balances (utxo, address, quantity)
            VALUES (:utxo, :address, :quantity)
            ON CONFLICT (utxo) DO UPDATE SET quantity = quantity + :quantity
        """
        cursor.execute(sql, movement)
        if movement["source"] != "reward":
            # update source balance
            cursor.execute("UPDATE balances SET quantity = quantity - :quantity WHERE utxo = :source", movement)


def regenerate_balances(db):
    print("Regenerating balances...")

    cursor = db.cursor()
    cursor.execute("DELETE FROM balances")

    cursor.execute("""
        INSERT INTO balances (utxo, address, quantity)
        SELECT 
            (txid || ':' || destination) AS utxo,
            destination_address,
            SUM(quantity) AS quantity
        FROM movements 
        GROUP BY txid, destination
    """)
    
    cursor.execute("""
        SELECT source, SUM(movements.quantity) AS quantity
        FROM movements
        GROUP BY source
    """)
    debits = cursor.fetchall()

    for debit in debits:
        cursor.execute(
            "UPDATE balances SET quantity = quantity - ? WHERE utxo = ?", 
            (debit["quantity"], debit["source"])
        )


def get_parsed_block(db, height):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM blocks WHERE height = ?", (height,))
    return cursor.fetchone()


def handle_reorg(db, height):
    print(f"Reorg detected at block {height}")
    while True:
        print(f"Reverting block {height}")
        cursor = db.cursor()
        cursor.execute("DELETE FROM movements WHERE height = ?", (height,))
        cursor.execute("DELETE FROM blocks WHERE height = ?", (height,))
        height -= 1
        block = get_decoded_block(height)
        previous_block = get_parsed_block(db, height - 1)
        if previous_block is None:
            cursor.execute("DELETE FROM movements")
            cursor.execute("DELETE FROM blocks")
            cursor.execute("DELETE FROM balances")
            break
        elif block["previousblockhash"] == previous_block["hash"]:
            print(f"Reorg fixed at block {height}")
            regenerate_balances(db)
            break
    catch_up(db)


def parse_block(db, height):
    print(f"Parsing block {height}", end="\r")
    block = get_decoded_block(height)
    previous_block = get_parsed_block(db, height - 1)
    if previous_block:
        if block["previousblockhash"] != previous_block["hash"]:
            return handle_reorg(db, height)

    #print(block)
    movement_count = process_movements(db, block)
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO blocks (height, hash, movement_count) VALUES (?, ?, ?)
    """, (height, block["hash"], movement_count))


def get_last_parsed_block_height(db):
    cursor = db.cursor()
    cursor.execute("SELECT height FROM blocks ORDER BY height DESC LIMIT 1")
    last_block = cursor.fetchone()
    if last_block:
        return last_block["height"]
    return -1


def parse_next_block(db):
    block_count = rpc_call("getblockcount", [])
    last_parsed_block_height = get_last_parsed_block_height(db)
    if last_parsed_block_height >= block_count:
        return False
    with db:
        parse_block(db, last_parsed_block_height + 1)
    return True


def catch_up(db):
    while parse_next_block(db):
        pass


def watch_for_new_block(db):
    print("Waiting for new block")
    while True:
        parse_next_block(db)
        time.sleep(1)


def start_parser(network_name):
    print(f"Starting parser for {network_name}")
    Config().set_network(network_name)
    db = connect_db()
    init_db(db)
    catch_up(db)

    #balances1 = db.cursor().execute("SELECT * FROM balances ORDER BY utxo, quantity").fetchall()
    #regenerate_balances(db)
    #balances2 = db.cursor().execute("SELECT * FROM balances ORDER BY utxo, quantity").fetchall()
    #if balances1 != balances2:
    #    print(balances1)
    #    print(balances2)
    #    raise Exception("Balances mismatch")

    watch_for_new_block(db)


def tests():
    assert calculate_reward(8, 9) == 25600000000
    assert calculate_reward(7, 9) == 1600000000
    assert calculate_reward(6, 9) == 100000000
    assert calculate_reward(5, 9) == 0
    assert calculate_reward(6, 6) == 409600000000

    assert calculate_distribution(100, [0, 0, 0]) == [100, 0, 0]
    assert calculate_distribution(100, [0, 0, 100]) == [0, 0, 100]
    assert calculate_distribution(100, [0, 100, 0]) == [0, 100, 0]
    assert calculate_distribution(100, [100, 0, 0]) == [100, 0, 0]
    assert calculate_distribution(100, [500, 1000]) == [34, 66]
    assert calculate_distribution(100, [1, 1, 1]) == [34, 33, 33]




if __name__ == "__main__":
    if len(sys.argv) < 2:
        network_name = "regtest"
    else:
        network_name = sys.argv[1]
    start_parser(network_name)

