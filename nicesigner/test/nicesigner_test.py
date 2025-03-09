from nicesigner import nicesigner

from bitcoinutils.transactions import Transaction


def test_transaction_generation():
    # Un mnémonique de test (NE PAS UTILISER EN PRODUCTION)
    mnemonic = (
        "addict weather world sense idle purity rich wagon ankle fall cheese spatial"
    )

    # Données de test avec script P2WPKH correct (commence par 0014)
    inputs = "d54994e544d31d7aa8bb2bfdcc16a4438f169c51d50bc876394c5fdc289c4aa9:1:1000000:0014d0c59903c5bac2868760e90fd521a4665aa76520"
    outputs = ""

    # Paramètres
    base_path = "m/84'/0'/0'/0"
    first_thread = 0
    num_threads = 4
    total_threads = 4
    target = 5  # Cherche un txid commençant par 0
    min_value = 546

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
        min_value,
    )
    print(tx_hex)
    print(derivation_path)

    tx = Transaction.from_raw(tx_hex)
    print(tx.get_txid())
    print(tx.get_vsize())

    # Vérifications
    assert tx_hex is not None
    assert len(tx_hex) > 0
    assert derivation_path.startswith(base_path)
    assert tx_hex.isalnum()  # Vérifie que c'est bien une chaîne hexadécimale


test_transaction_generation()
