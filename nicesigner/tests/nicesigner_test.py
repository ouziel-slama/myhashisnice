from bitcoinutils.transactions import Transaction

import nicesigner

def test_nicesigner():
    
    inputs = ":".join([
        "964b06c7d65bc2966ffc089be06469cf3961fdae4253cb51fe158bf1696882a1",
        "1",
        "100000",
        "0014841b80d2cc75f5345c482af96294d04fdd66b2b7",
    ])
    outputs = ":".join([
        "100000",
        "76a91402245e1265ca65f5ab6d70289f7bcfed6204810588ac",
    ])
    private_key = "027b11e21a61ca5971db8dea640fa6959eb0be203773d8afa27621ba558da831"
    num_threads = 4;
    target = 4;
    raw_tx = nicesigner.nicesigner.build_transaction(inputs, outputs, private_key, num_threads, target);

    tx = Transaction.from_raw(raw_tx)
    print(tx.get_txid())

    print(raw_tx)
