from decimal import Decimal as D

from flask import Flask, render_template, url_for, request

from lib import CONFIG, Config, APSWConnectionPool

app = Flask(__name__)

NETWORK_NAME = "regtest"
config = CONFIG[NETWORK_NAME]

Config().set_network(NETWORK_NAME)


def get_all_movements(db, limit=100, offset=0):
    sql = """
        SELECT height, source, (txid || ':' || destination) as destination, quantity 
        FROM movements 
        WHERE source != 'reward'
        ORDER BY rowid 
        DESC LIMIT :limit OFFSET :offset
    """
    cursor = db.cursor()
    cursor.execute(sql, {"limit": limit, "offset": offset})
    return cursor.fetchall()

def get_all_rewards(db, limit=100, offset=0):
    sql = """
        SELECT height, source, (txid || ':' || destination) as destination, quantity 
        FROM movements 
        WHERE source = 'reward'
        ORDER BY rowid 
        DESC LIMIT :limit OFFSET :offset
    """
    cursor = db.cursor()
    cursor.execute(sql, {"limit": limit, "offset": offset})
    return cursor.fetchall()


def get_balances_by_address(db, address):
    sql = """
        SELECT * FROM balances WHERE address = :address AND quantity > 0 ORDER BY quantity DESC
    """
    cursor = db.cursor()
    cursor.execute(sql, {"address": address})
    return cursor.fetchall()


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
    if utxo == "reward":
        return "reward"
    if full:
        return bold_zero(utxo)
    return bold_zero(f"{utxo[:12]}...{utxo[-12:]}")


def display_quantity(quantity):
    value = D(quantity) / D(config["UNIT"])
    return "{0:.8f} MHIN".format(value)


@app.route("/")
def home():
    with APSWConnectionPool(Config()["DATABASE_FILE"]).connection() as db:
        movements = get_all_movements(db)
        rewards = get_all_rewards(db)
    return render_template('home.html', 
        movements=movements,
        rewards=rewards,
        display_utxo=display_utxo,
        display_quantity=display_quantity,
        css_url=url_for('static', filename='home.css')
    )

@app.route("/protocol")
def protocol():
    return render_template('protocol.html',
        css_url=url_for('static', filename='home.css')
    )

@app.route("/balances")
def balances():
    address = request.args.get('address')
    if address is not None:
        with APSWConnectionPool(Config()["DATABASE_FILE"]).connection() as db:
            balances = get_balances_by_address(db, address)
            balance_total = sum([D(balance["quantity"]) for balance in balances])
    else:
        balances = []
        balance_total = None
    return render_template('balances.html',
        display_utxo=display_utxo,
        display_quantity=display_quantity,
        balances=balances,
        balance_total=balance_total,
        css_url=url_for('static', filename='home.css'),
        address=address
    )