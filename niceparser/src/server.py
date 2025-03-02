from decimal import Decimal as D
import binascii
from contextlib import contextmanager

from flask import Flask, render_template, url_for, request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash


from config import Config
from queries import MhinQueries


from nicefetcher import utils

app = Flask(__name__)
auth = HTTPBasicAuth()

users = {
    "admin": generate_password_hash("myassisnice2025"),
}

NETWORK_NAME = "mainnet"

Config().set_network(NETWORK_NAME)




@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username


def utxo_to_utxo_id(txid, n):
    txid = utils.inverse_hash(txid)
    txid = binascii.unhexlify(txid)
    return bytes(utils.pack_utxo(txid, n))


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
    value = D(quantity) / D(Config()["UNIT"])
    return "{0:.8f} MHIN".format(value)


@app.route("/")
@auth.login_required
def home():
    return render_template(
        "home.html",
        rewards=MhinQueries().get_latest_nicehashes(),
        stats=MhinQueries().get_stats(),
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
    balance = MhinQueries().get_balance_by_address(address)
    return render_template(
        "balances.html",
        display_utxo=display_utxo,
        display_quantity=display_quantity,
        balance=balance,
        css_url=url_for("static", filename="home.css"),
        address=address,
    )


if __name__ == "__main__":
    app.run(host='127.0.0.1')
