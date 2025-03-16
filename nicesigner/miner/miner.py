import json
import time
import json

import sh

from bitcoinutils.transactions import Transaction
from bitcoinutils.keys import P2wpkhAddress
from bitcoinutils.hdwallet import HDWallet
from bitcoinutils.setup import setup

from nicesigner import nicesigner

setup("mainnet")

DUST_SIZE = 550

RPC_USER = "rpc"
RPC_PASSWORD = "rpc"
TARGET = 6
TOTAL_THREADS = 32

bitcoin_cli = sh.bitcoin_cli.bake(
    f"-rpcuser={RPC_USER}",
    f"-rpcpassword={RPC_PASSWORD}",
)


def build_nice_transaction(mnemonic, utxo_txid, utxo_vout, utxo_value, utxo_path):
    utxo_address = get_hdwallet_address(mnemonic, utxo_path)
    script_pubkey = P2wpkhAddress(utxo_address).to_script_pub_key().to_hex()
    inputs = f"{utxo_txid}:{utxo_vout}:{utxo_value}:{script_pubkey}:{utxo_path}"
    outputs = ""

    base_path = "m/84'/0'/0'/0"
    first_thread = 0
    num_threads = TOTAL_THREADS
    total_threads = TOTAL_THREADS
    target = TARGET
    output_value = utxo_value - 330  # vsize 110 * 3
    if output_value < DUST_SIZE:
        raise ValueError("Output value is too low")

    # Génération de la transaction
    tx_hex, derivation_path = nicesigner.build_transaction(
        inputs,
        outputs,
        mnemonic,
        base_path,
        first_thread,
        num_threads,
        total_threads,
        target,
        output_value,
    )

    return tx_hex, derivation_path, output_value


def get_hdwallet_address(mnemonic, derivation_path):
    hdw = HDWallet(mnemonic=mnemonic)
    hdw.from_path(derivation_path)
    address = hdw.get_private_key().get_public_key().get_segwit_address().to_string()
    return address


def backup_tx(batch_name, txid, derivation_path, utxo_value, tx_hex):
    with open(f"{batch_name}-{txid}.json", "w") as f:
        json.dump(
            {
                "txid": txid,
                "raw_tx": tx_hex,
                "path": derivation_path,
                "value": utxo_value,
            },
            f,
        )
    print(f"Backuped tx {txid} to {batch_name}-{txid}.json")


def mint_mihn(
    mnemonic, utxo_txid, utxo_vout, utxo_value, utxo_path, batch_name, counter=1
):
    next_utxo_txid = utxo_txid
    next_utxo_vout = utxo_vout
    next_utxo_value = utxo_value
    next_utxo_path = utxo_path
    while next_utxo_value > DUST_SIZE + 330:
        tx_hex, next_utxo_path, next_utxo_value = build_nice_transaction(
            mnemonic, next_utxo_txid, next_utxo_vout, next_utxo_value, next_utxo_path
        )
        tx = Transaction.from_raw(tx_hex)
        next_utxo_txid = tx.get_txid()
        next_utxo_vout = 0
        while True:
            try:
                sent_txid = bitcoin_cli("sendrawtransaction", tx_hex).strip()
                break
            except sh.ErrorReturnCode_26 as e:
                print("Resending tx in 2 minutes...")
                time.sleep(120)
        backup_tx(
            f"{batch_name}-{counter}",
            next_utxo_txid,
            next_utxo_path,
            next_utxo_value,
            tx_hex,
        )
        print(f"Sent tx {sent_txid}")
        assert sent_txid == next_utxo_txid
        counter += 1



# use genwallet.py to generate mnemonic and address
mnemonic = "addict weather world sense idle purity rich wagon ankle fall cheese spatial"
address_path = "m/84'/0'/0'/0/1"

txid = "" # utxo txid
vout = 0 # utxo vout
value = 84807 # utxo value

batch_name = "mybatch"


mint_mihn(mnemonic, txid, vout, value, address_path, batch_name)
