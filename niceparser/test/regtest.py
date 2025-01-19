import os
import shutil
import sys
import time
import signal
import json

import sh

from bitcoinutils.keys import PrivateKey, P2wpkhAddress
from bitcoinutils.setup import setup
from bitcoinutils.transactions import Transaction

import nicesigner

setup("regtest")

DATA_DIR = "niceparserregtest"

if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)
os.makedirs(DATA_DIR)

random = os.urandom(32)
priv = PrivateKey(b=random)
pubkey = priv.get_public_key()
address = pubkey.get_segwit_address().to_string()
private_key = priv.to_bytes().hex()
print("Private key:", private_key)
print("Public key:", pubkey.to_hex(compressed=True))
print("Address:", address)

random = os.urandom(32)
priv = PrivateKey(b=random)
pubkey = priv.get_public_key()
address2 = pubkey.get_segwit_address().to_string()
private_key2 = priv.to_bytes().hex()
print("Private key:", private_key)
print("Public key:", pubkey.to_hex(compressed=True))
print("Address:", address)


bitcoin_cli = sh.bitcoin_cli.bake(
    "-regtest",
    "-rpcuser=rpc",
    "-rpcpassword=rpc",
    "-rpcconnect=localhost",
    f"-datadir={DATA_DIR}",
)


def wait_for_bitcoind():
    while True:
        try:
            bitcoin_cli("getblockchaininfo")
            break
        except sh.ErrorReturnCode:
            print("Waiting for bitcoind to start...")
            time.sleep(1)

def start_bitcoind():
    try:
        bitcoind_process = sh.bitcoind(
            "-regtest",
            "-daemon",
            "-server",
            "-txindex",
            "-rpcuser=rpc",
            "-rpcpassword=rpc",
            "-fallbackfee=0.0002",
            "-acceptnonstdtxn",
            "-minrelaytxfee=0",
            "-blockmintxfee=0",
            "-mempoolfullrbf",
            f"-datadir={DATA_DIR}",
            _bg=True,
            _out=sys.stdout,
        )
        
        wait_for_bitcoind()
        print("Regtest bitcoind ready")
        block_hashes_json = json.loads(bitcoin_cli("generatetoaddress", 120, address).strip())
        print("Block hashes:", block_hashes_json)


        block = json.loads(bitcoin_cli("getblock", block_hashes_json[0], 2).strip())

        txid = block["tx"][0]["txid"]
        vout = 0
        input_value = int(block["tx"][0]["vout"][0]["value"] * 10**8)
        input_lock_script = block["tx"][0]["vout"][0]["scriptPubKey"]["hex"]
        output_value = input_value - 200
        output_lock_script = P2wpkhAddress(address).to_script_pub_key().to_hex()

        inputs = f"{txid}:{vout}:{input_value}:{input_lock_script}"
        outputs = f"{output_value}:{output_lock_script}"
        
        print("Inputs:", inputs)
        print("Outputs:", outputs)

        num_threads = 4;
        target = 4;
        raw_tx = nicesigner.nicesigner.build_transaction(inputs, outputs, private_key, num_threads, target)
        txid = bitcoin_cli("sendrawtransaction", raw_tx).strip()
        #bitcoin_cli("generatetoaddress", 1, address)

        print("TXID:", txid)

        input_value = output_value
        input_lock_script = output_lock_script
        inputs = f"{txid}:{vout}:{input_value}:{input_lock_script}"
        output_value = input_value - 200
        outputs = f"{output_value}:{output_lock_script}"

        print("Inputs:", inputs)
        print("Outputs:", outputs)
        target = 3;
        raw_tx = nicesigner.nicesigner.build_transaction(inputs, outputs, private_key, num_threads, target)
        txid = bitcoin_cli("sendrawtransaction", raw_tx).strip()
        #bitcoin_cli("generatetoaddress", 1, address)

        print("TXID:", txid)

        input_value = output_value
        inputs = f"{txid}:{vout}:{input_value}:{input_lock_script}"
        output_value = input_value - 200
        outputs = f"{output_value}:{output_lock_script}"

        print("Inputs:", inputs)
        print("Outputs:", outputs)
        target = 3;
        raw_tx = nicesigner.nicesigner.build_transaction(inputs, outputs, private_key, num_threads, target)
        txid = bitcoin_cli("sendrawtransaction", raw_tx).strip()

        input_value = output_value
        inputs = f"{txid}:{vout}:{input_value}:{input_lock_script}"
        output_value_1 = int(input_value / 3)
        output_lock_script_1 = output_lock_script
        output_value_2 = int(input_value / 3)
        output_lock_script_2 = P2wpkhAddress(address2).to_script_pub_key().to_hex()
        output_value_3 = int(input_value / 3 - 200)
        output_lock_script_3 = output_lock_script
        outputs = ",".join([
            f"{output_value_1}:{output_lock_script_1}",
            f"{output_value_2}:{output_lock_script_2}",
            f"{output_value_3}:{output_lock_script_3}",
        ])

        print("Inputs:", inputs)
        print("Outputs:", outputs)
        target = 3;
        raw_tx = nicesigner.nicesigner.build_transaction(inputs, outputs, private_key, num_threads, target)
        txid = bitcoin_cli("sendrawtransaction", raw_tx).strip()


        bitcoin_cli("generatetoaddress", 1, address)

        print("TXID:", txid)

        while True:
            time.sleep(1)
    finally:
        bitcoin_cli("stop")


start_bitcoind()

