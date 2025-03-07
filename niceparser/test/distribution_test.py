def calculate_distribution(value, output_values):
    total_output = sum(output_values)
    if total_output == 0:
        return [value] + [0] * (len(output_values) - 1)

    distribution = [
        int(D(value) * (D(output_value) / D(total_output)))
        for output_value in output_values
    ]
    total_distributed = sum(distribution)

    if total_distributed < value:
        distribution[0] += value - total_distributed

    return distribution


def calculate_reward(zero_count, max_zero_count):
    if zero_count < Config()["MIN_ZERO_COUNT"]:
        return 0
    reward = D(Config()["MAX_REWARD"]) / D(16 ** (max_zero_count - zero_count))
    return int(reward)


from decimal import Decimal as D


def tests():
    # assert calculate_reward(8, 9) == 25600000000
    # assert calculate_reward(7, 9) == 1600000000
    # assert calculate_reward(6, 9) == 100000000
    # assert calculate_reward(5, 9) == 0
    # assert calculate_reward(6, 6) == 409600000000

    assert calculate_distribution(100, [0, 0, 0]) == [100, 0, 0]
    assert calculate_distribution(100, [0, 0, 100]) == [0, 0, 100]
    assert calculate_distribution(100, [0, 100, 0]) == [0, 100, 0]
    assert calculate_distribution(100, [100, 0, 0]) == [100, 0, 0]
    assert calculate_distribution(100, [500, 1000]) == [34, 66]
    assert calculate_distribution(100, [1, 1, 1]) == [34, 33, 33]
    assert calculate_distribution(1, [50, 50, 50, 50]) == [1, 0, 0, 0]
    assert calculate_distribution(2, [50, 50, 50, 50]) == [2, 0, 0, 0]
    assert calculate_distribution(3, [50, 50, 50, 50]) == [3, 0, 0, 0]
    assert calculate_distribution(4, [50, 50, 50, 50]) == [1, 1, 1, 1]


import os
import struct
import tempfile
import unittest
import binascii


def test_utxo_set():
    # Créer un fichier temporaire pour éviter de modifier des fichiers existants
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_backup_path = temp_file.name

    try:
        # Modifier temporairement la configuration pour utiliser le fichier temporaire
        Config().set_network("mainnet")
        # Config()["UTXOSET_DB"] = temp_backup_path

        key1 = binascii.unhexlify("a" * 16)
        key2 = binascii.unhexlify("b" * 16)
        key3 = binascii.unhexlify("c" * 16)

        # Test 1: Ajout d'UTXOs
        UTXOSet().add(key1, 1000)
        UTXOSet().add(key2, 2000)

        # Vérifier que les UTXOs ont été ajoutés correctement
        assert UTXOSet().get_balance(key1) == 1000
        assert UTXOSet().get_balance(key2) == 2000
        assert UTXOSet().get_balance(b"nonexistent") == 0

        # Test 2: Suppression d'UTXOs
        UTXOSet().remove(key1)
        assert UTXOSet().get_balance(key1) == 0
        assert UTXOSet().get_balance(key2) == 2000

        print("state1", UTXOSet().utxo_set)

        # Fermer le fichier de sauvegarde
        UTXOSet().close()

        print("state2", UTXOSet().utxo_set)

        # Test 3: Rechargement du fichier de sauvegarde
        UTXOSet().init()

        print("state3", UTXOSet().utxo_set)

        # Vérifier que les données persistent
        assert UTXOSet().get_balance(key1) == 0
        assert UTXOSet().get_balance(key2) == 2000

        # Test 4: Ajout et suppression multiples
        UTXOSet().add(key3, 3000)
        UTXOSet().remove(key2)

        UTXOSet().close()

        # Test 5: Vérifier le rechargement après modifications
        UTXOSet().init()
        assert UTXOSet().get_balance(key3) == 3000
        assert UTXOSet().get_balance(key2) == 0

        print("Tous les tests de UTXOSet ont réussi !")

    finally:
        # Nettoyer le fichier temporaire
        if os.path.exists(temp_backup_path):
            os.unlink(temp_backup_path)
