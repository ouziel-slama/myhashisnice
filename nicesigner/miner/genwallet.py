from mnemonic import Mnemonic
from bitcoinutils.hdwallet import HDWallet
from bitcoinutils.setup import setup

setup("mainnet")

mnemo = Mnemonic("english")
mnemonic = mnemo.generate()

derivation_path = "m/84'/0'/0'/0/1"

hdw = HDWallet(mnemonic=mnemonic)
hdw.from_path(derivation_path)
address = hdw.get_private_key().get_public_key().get_segwit_address().to_string()

print(f"mnemonic: {mnemonic}")
print(f"address: {address}")
print(f"derivation_path: {derivation_path}")