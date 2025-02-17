from decimal import Decimal as D
import binascii
import datetime
import os
import json


from lib import CONFIG, Config, APSWConnectionPool, SingletonMeta


from nicefetcher import utils


NETWORK_NAME = "mainnet"
config = CONFIG[NETWORK_NAME]

Config().set_network(NETWORK_NAME)


def get_supply(db):
    select_queries = [
        f"SELECT balance FROM utxos_{i} WHERE reward IS TRUE"
        for i in range(10)
    ]
    
    sql = f"""
    WITH all_rewards AS (
        {' UNION ALL '.join(select_queries)}
    )
    SELECT SUM(balance) as total_supply 
    FROM all_rewards
    """
    
    result = db.execute(sql).fetchone()
    return result["total_supply"] or 0  # Retourne 0 si le résultat est NULL


def get_block_with_reward(db):
    select_queries = [
        f"SELECT COUNT(DISTINCT height) AS count FROM utxos_{i} WHERE reward IS TRUE"
        for i in range(10)
    ]
    sql = f"""
    WITH all_rewards AS (
        {' UNION ALL '.join(select_queries)}
    )
    SELECT SUM(count) as block_with_reward
    FROM all_rewards
    """
    result = db.execute(sql).fetchone()
    return result["block_with_reward"] or 0

def get_block_with_max_reward(db):
    select_queries = [
        f"SELECT height, COUNT(*) AS count FROM utxos_{i} WHERE reward IS TRUE GROUP BY height"
        for i in range(10)
    ]
    sql = f"""
    WITH all_rewards AS (
        {' UNION ALL '.join(select_queries)}
    )
    SELECT height, count
    FROM all_rewards
    ORDER BY count DESC
    LIMIT 1
    """
    result = db.execute(sql).fetchone()
    return result["height"], result["count"] or 0


def get_holders_count(db):
    # Construire les requêtes pour chaque table
    select_all = []
    select_spent = []
    for i in range(10):
        select_all.append(f"""
            SELECT address_hash, utxo_id, balance 
            FROM utxos_{i}
        """)
        select_spent.append(f"""
            SELECT utxo_id
            FROM utxos_{i} 
            WHERE source_utxo_id IN (SELECT utxo_id FROM all_utxos)
        """)
    
    select_all = " UNION ALL ".join(select_all)
    select_spent = " UNION ALL ".join(select_spent)

    sql = f"""
    WITH 
        all_utxos AS ({select_all}),
        spent_utxos AS ({select_spent}),
        active_utxos AS (
            SELECT address_hash, SUM(balance) as total_balance
            FROM all_utxos
            WHERE utxo_id NOT IN (SELECT utxo_id FROM spent_utxos)
            GROUP BY address_hash
        )
    SELECT COUNT(*) as address_count
    FROM active_utxos
    WHERE total_balance > 0
    """
    
    result = db.execute(sql).fetchone()
    return result["address_count"]


def get_nicest_hash(db):
    sql = f"""
    SELECT txid FROM txids 
    ORDER BY zero_count DESC, height ASC, position ASC
    LIMIT 1
    """
    result = db.execute(sql).fetchone()
    return result["txid"]


def get_first_nice_hash(db):
    sql = f"""
    SELECT txid FROM txids WHERE zero_count >= {Config()["MIN_ZERO_COUNT"]}
    ORDER BY height ASC, position ASC
    LIMIT 1
    """
    result = db.execute(sql).fetchone()
    return result["txid"]


def get_last_nice_hash(db):
    sql = f"""
    SELECT txid FROM txids WHERE zero_count >= {Config()["MIN_ZERO_COUNT"]}
    ORDER BY height DESC, position DESC
    LIMIT 1
    """
    result = db.execute(sql).fetchone()
    return result["txid"]


def get_nice_hash_count(db):
    sql = f"""
    SELECT COUNT(*) as nice_hash_count FROM txids WHERE zero_count >= {Config()["MIN_ZERO_COUNT"]}
    """
    result = db.execute(sql).fetchone()
    return result["nice_hash_count"]


def generate_stats():

    with APSWConnectionPool(Config()["DATABASE_FILE"]).connection() as db:

        start_time =  datetime.datetime.now()
        height, count = get_block_with_max_reward(db)
        print("Block with max reward", height, count)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        block_with_reward = get_block_with_reward(db)
        print("Block with reward", block_with_reward)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        nice_hashes_count = get_nice_hash_count(db)
        print("Nice hashes count", nice_hashes_count)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        supply = get_supply(db)
        print("Supply", supply)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        nicest_hash = get_nicest_hash(db)
        nicest_hash = utils.inverse_hash(binascii.hexlify(nicest_hash).decode("utf-8"))
        print("Nicest hash", nicest_hash)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        first_nice_hash = get_first_nice_hash(db)
        first_nice_hash = utils.inverse_hash(binascii.hexlify(first_nice_hash).decode("utf-8"))
        print("First nice hash", first_nice_hash)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        start_time =  datetime.datetime.now()
        last_nice_hash = get_last_nice_hash(db)
        last_nice_hash = utils.inverse_hash(binascii.hexlify(last_nice_hash).decode("utf-8"))
        print("Last nice hash", last_nice_hash)
        end_time = datetime.datetime.now()
        print("Time elapsed", (end_time - start_time))

        result = {
            "supply": supply,
            "nicest_hash": nicest_hash,
            "first_nice_hash": first_nice_hash,
            "last_nice_hash": last_nice_hash,
            "nice_hashes_count": nice_hashes_count,
            "block_with_reward": block_with_reward,
            "block_with_max_reward": {
                "height": height,
                "count": count
            }
        }
        stats_path = os.path.join(Config()["DATA_DIR"], "stats.json")
        with open(stats_path, "w") as f:
            print(result)
            json.dump(result, f, indent=4)

    return result


if __name__ == "__main__":
    generate_stats()