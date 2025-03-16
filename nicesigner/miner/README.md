# Prerequisites:

- An up-to-date Bitcoin Core instance with `bitcoin-cli` in your path
- Rust

## Installation

1. Download and install `nicesigner`:

```
git clone https://github.com/ouziel-slama/myhashisnice
cd myhashisnice/nicesigner
pip3 install maturin
pip3 install -e .
```

2. Generate a wallet:

```
$ python3 miner/genwallet.py
mnemonic: slight brother long eight eyebrow anchor lawn combine awesome exit bone knife
address: bc1qg5pwp4zzc8zc7nfyamgmp24dpe9frrqnq9qfm4
derivation_path: m/84'/0'/0'/0/1
```
Take the usual precautions to protect your mnemonic.

3. Fund your mining address

Send the number of satoshis you want to mine to the address you generated in the previous step. The miner is configured to pay 330 sats in fees per transaction.

4. Prepare the `miner.py` file

In the `miner/miner.py` file, update the following variables at the beginning of the file:

```
RPC_USER = "rpc"      # the RPC username of your bitcoin core
RPC_PASSWORD = "rpc"  # the password
TARGET = 6            # the number of zeros starting the mined transactions
TOTAL_THREADS = 32    # the number of threads to use
```

and at the end of the file:

```
# the mnemonic and path generated in step 2
mnemonic = "addict weather world sense idle purity rich wagon ankle fall cheese spatial"
address_path = "m/84'/0'/0'/0/1"

# the txid, vout and value of the transaction used to fund your mining address.
# you can find this information on mempool.space for example
txid = ""            
vout = 0            # utxo vout
value = 84807       # utxo value

# prefix of files generated for each mined transaction
batch_name = "mybatch"
```

5. Mine!

```
python3 miner/miner.py
```

The miner will generate transactions as long as there are enough satoshis left. The MHINs accumulate in the address of the last mined transaction! You'll find the derivation path of this address in the corresponding file.

Happy mining!