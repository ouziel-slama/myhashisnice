from decimal import Decimal as D
import binascii
import datetime
import os
import json

from flask import Flask, render_template, url_for, request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash


from lib import CONFIG, Config, APSWConnectionPool, SingletonMeta


from nicefetcher import utils

app = Flask(__name__)
auth = HTTPBasicAuth()

users = {
    "admin": generate_password_hash("myassisnice2025"),
}

NETWORK_NAME = "mainnet"
config = CONFIG[NETWORK_NAME]

Config().set_network(NETWORK_NAME)

class Cache(metaclass=SingletonMeta):
    def __init__(self):
        self.last_100_rewards = None
        self.last_100_rewards_height = 0
        self.last_100_movements = None
        self.last_100_movements_height = 0
        self.balances = {}


@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username


def get_utxo(db, txid, n):
    txid = utils.inverse_hash(txid)
    txid = binascii.unhexlify(txid)
    utxo_id = bytes(utils.pack_utxo(txid, n))
    for i in range(10):
        sql = f"SELECT * FROM utxos_{i} WHERE utxo_id = :utxo_id"
        utxo = db.execute(sql, {"utxo_id": utxo_id}).fetchone()
        if utxo:
            return utxo
    return None


def get_balance(db, address):
    address_hash = bytes(utils.pack_address(address))
    print("address_hash", address_hash)

    select_all = []
    select_spent = []
    for i in range(10):
        select_all.append(f"SELECT utxo_id, balance, address_hash FROM utxos_{i} WHERE address_hash = :address_hash")
        select_spent.append(f"SELECT source_utxo_id FROM utxos_{i} WHERE source_utxo_id IN (SELECT utxo_id FROM all_utxos)")
    select_all = " UNION ALL ".join(select_all)
    select_spent = " UNION ALL ".join(select_spent)
        
    sql = f"""
    WITH
        all_utxos AS ({select_all}),
        spent_utxos AS ({select_spent})
    SELECT 
        (SELECT COALESCE(SUM(balance), 0) FROM all_utxos WHERE utxo_id NOT IN (SELECT source_utxo_id FROM spent_utxos)) as balance
    """
    return db.execute(sql, {"address_hash": address_hash}).fetchone()["balance"]



def get_address_utxos(db, address):
    address_hash = bytes(utils.pack_address(address))
    print("address_hash", address_hash)

    select_all = []
    select_spent = []
    for i in range(10):
        select_all.append(f"SELECT utxo_id, balance, address_hash, height, position, vout FROM utxos_{i} WHERE address_hash = :address_hash")
        select_spent.append(f"SELECT source_utxo_id, height, position FROM utxos_{i} WHERE source_utxo_id IN (SELECT utxo_id FROM all_utxos)")
    select_all = " UNION ALL ".join(select_all)
    select_spent = " UNION ALL ".join(select_spent)
        
    sql = f"""
    WITH
        all_utxos AS ({select_all}),
        spent_utxos AS ({select_spent})
    SELECT u.*, t.txid FROM all_utxos u
    JOIN txids t ON u.height = t.height AND u.position = t.position
    WHERE u.utxo_id NOT IN (SELECT source_utxo_id FROM spent_utxos)
    """
    results = db.execute(sql, {"address_hash": address_hash}).fetchall()
    total = sum([utxo["balance"] for utxo in results])

    final_results = []
    for result in results:
        txid = utils.inverse_hash(binascii.hexlify(result["txid"]).decode("utf-8"))
        utxo = f"{txid}:{result['vout']}"
        final_results.append({
            "height": result["height"],
            "destination": utxo,
            "quantity": result["balance"],
        })

    return final_results, total


def get_last_rewards(db):
    cursor = db.cursor()

    last_block_height = cursor.execute(
        "SELECT height FROM blocks ORDER BY height DESC LIMIT 1"
    ).fetchone()["height"]

    if Cache().last_100_rewards_height == last_block_height:
        return Cache().last_100_rewards

    sql = "SELECT height, position FROM txids WHERE zero_count >= 6 ORDER BY height DESC LIMIT 20"
    last_100 = cursor.execute(sql).fetchall()
    queries = []
    bindings = []
    for txid in last_100:
        queries.append(f"""
            SELECT u.*, t.txid FROM utxos_{txid["height"] % 10} u
            JOIN txids t ON u.height = t.height AND u.position = t.position
            WHERE u.height = ? AND u.position = ?
        """)
        bindings += [txid["height"], txid["position"]]
    union_all = " UNION ALL ".join(queries)
    sql = f"SELECT * FROM ({union_all}) ORDER BY height DESC, position DESC"
    cursor.execute(sql, bindings)
    results = cursor.fetchall()

    final_results = []
    for result in results:
        txid = utils.inverse_hash(binascii.hexlify(result["txid"]).decode("utf-8"))
        utxo = f"{txid}:{result['vout']}"
        final_results.append({
            "height": result["height"],
            "destination": utxo,
            "quantity": result["balance"],
        })

    Cache().last_100_rewards_height = last_block_height
    Cache().last_100_rewards = final_results

    return final_results


def get_last_movements(db):
    cursor = db.cursor()

    last_block_height = cursor.execute(
        "SELECT height FROM blocks ORDER BY height DESC LIMIT 1"
    ).fetchone()["height"]

    if Cache().last_100_movements_height == last_block_height:
        return Cache().last_100_movements
    
    sql = "SELECT height, position FROM txids WHERE zero_count < 6 ORDER BY height DESC LIMIT 20"
    last_100 = cursor.execute(sql).fetchall()
    queries = []
    bindings = []
    for txid in last_100:
        queries.append(f"""
            SELECT u.*, t.txid FROM utxos_{txid["height"] % 10} u
            JOIN txids t ON u.height = t.height AND u.position = t.position
            WHERE u.height = ? AND u.position = ? AND u.balance > 0
        """)
        bindings += [txid["height"], txid["position"]]
    union_all = " UNION ALL ".join(queries)
    sql = f"SELECT * FROM ({union_all}) ORDER BY height DESC, position DESC"
    cursor.execute(sql, bindings)
    results = cursor.fetchall()

    source_utxos_ids = []
    source_utxos = []
    for result in results:
        source_utxos_ids.append(result["source_utxo_id"])
    for i in range(10):
        sql = f"""
            SELECT *, t.txid FROM utxos_{i} u
            JOIN txids t ON u.height = t.height AND u.position = t.position
            WHERE u.utxo_id IN ({','.join(['?'] * len(source_utxos_ids))})
        """
        cursor.execute(sql, source_utxos_ids)
        source_utxos += cursor.fetchall()
    
    source_by_utxo_id = {}
    for source_utxo in source_utxos:
        source_by_utxo_id[source_utxo["utxo_id"]] = source_utxo

    final_results = []
    for result in results:
        source_txid = source_by_utxo_id[result["source_utxo_id"]]["txid"]
        source_txid = utils.inverse_hash(binascii.hexlify(source_txid).decode("utf-8"))
        source_utxo = f"{source_txid}:{source_by_utxo_id[result['source_utxo_id']]['vout']}"
        txid = utils.inverse_hash(binascii.hexlify(result["txid"]).decode("utf-8"))
        utxo = f"{txid}:{result['vout']}"
        final_results.append({
            "height": result["height"],
            "destination": utxo,
            "source": source_utxo,
            "quantity": result["balance"],
        })

    Cache().last_100_movements_height = last_block_height
    Cache().last_100_movements = final_results

    return final_results


def bold_zero(value):
    if not value.startswith("0"):
        return value
    bold_value = f"<b>"
    closed = False
    for char in value:
        if char != "0" and not closed:
            bold_value += f"</b>{char}"
            closed = True
        else:
            bold_value += f"{char}"
    return bold_value


def display_utxo(utxo, full=False):
    if ":" not in utxo:
        txid = utxo
    else:
        txid, _vout = utxo.split(":")
    if full:
        bolded = bold_zero(utxo)
    else:
        bolded = bold_zero(f"{utxo[:12]}...{utxo[-12:]}")
    linked = f'<a href="https://mempool.space/tx/{txid}" target="_blank">{bolded}</a>'
    return linked


def display_quantity(quantity):
    value = D(quantity) / D(config["UNIT"])
    return "{0:.8f} MHIN".format(value)


@app.route("/")
@auth.login_required
def home():
    with APSWConnectionPool(Config()["DATABASE_FILE"]).connection() as db:
        movements = get_last_movements(db)
        rewards = get_last_rewards(db)
        stats_path = os.path.join(Config()["DATA_DIR"], "stats.json")
        with open(stats_path, "r") as f:
            stats = json.loads(f.read())
    
    return render_template(
        "home.html",
        movements=movements,
        rewards=rewards,
        stats=stats,
        display_utxo=display_utxo,
        display_quantity=display_quantity,
        css_url=url_for("static", filename="home.css"),
    )


@app.route("/protocol")
@auth.login_required
def protocol():
    return render_template(
        "protocol.html", css_url=url_for("static", filename="home.css")
    )


@app.route("/balances")
@auth.login_required
def balances():
    address = request.args.get("address")
    if address is not None:
        with APSWConnectionPool(Config()["DATABASE_FILE"]).connection() as db:
            balances, balance_total = get_address_utxos(db, address)
    else:
        balances = []
        balance_total = None
    return render_template(
        "balances.html",
        display_utxo=display_utxo,
        display_quantity=display_quantity,
        balances=balances,
        balance_total=balance_total,
        css_url=url_for("static", filename="home.css"),
        address=address,
    )


if __name__ == "__main__":
    app.run(host='127.0.0.1')
