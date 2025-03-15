import binascii
from decimal import Decimal as D

from config import Config
from nicefetcher import utils


def calculate_reward(zero_count, max_zero_count):
    """
    Calculates the reward based on the number of zeros

    Args:
        zero_count (int): Number of zeros at the beginning of the hash
        max_zero_count (int): Maximum number of zeros observed

    Returns:
        int: Reward amount
    """
    if zero_count < Config()["MIN_ZERO_COUNT"]:
        return 0
    reward = D(Config()["MAX_REWARD"]) / D(16 ** (max_zero_count - zero_count))
    return int(reward)


def calculate_distribution(value, output_values):
    """
    Calculates the distribution of values among multiple outputs

    Args:
        value (int): Total value to distribute
        output_values (list): List of output values to use for weighting the distribution

    Returns:
        list: List of distributed amounts
    """
    total_output = sum(output_values)
    if total_output == 0:
        return [0] * len(output_values)

    distribution = [
        int(D(value) * (D(output_value) / D(total_output)))
        for output_value in output_values
    ]
    total_distributed = sum(distribution)

    if total_distributed < value:
        distribution[0] += value - total_distributed

    return distribution


def generate_movements(tx, quantity, output_values):
    """
    Generates fund movements for a transaction

    Args:
        tx (dict): Transaction information
        quantity (int): Total quantity to distribute
        output_values (list): Output values for weighting

    Returns:
        list: List of generated movements
    """
    movements = []
    distribution = calculate_distribution(quantity, output_values)
    
    # Identifier tous les outputs non-OP_RETURN
    valid_outputs = [out for out in tx["vout"] if not out["is_op_return"]]
    
    # Si plus d'un output valide, exclure le dernier
    if len(valid_outputs) > 1:
        valid_outputs = valid_outputs[:-1]
    
    # Créer des mouvements uniquement pour les outputs que nous voulons distribuer
    for i, out in enumerate(valid_outputs):
        if i < len(distribution):  # Vérification supplémentaire
            movements.append({
                "utxo_id": out["utxo_id"],
                "quantity": distribution[i],
            })
    
    return movements


def process_block_unified(block, mhin_store):
    """
    Processes a block using the MhinStore interface

    Args:
        block (dict): Block data to process
        mhin_store (MhinStore): MhinStore instance for storage and indexing
    """
    height = block["height"]
    mhin_store.start_block(height, block["block_hash"])

    for tx in block["tx"]:
        # exclude coinbase
        if "coinbase" in tx["vin"][0]:
            continue

        # exclude op_return
        output_values = [out["value"] for out in tx["vout"] if not out["is_op_return"]]
        if len(output_values) < 1:
            continue
        # don't distribute to the last output if there are more than one
        if len(output_values) > 1:
            output_values.pop()

        # reward movements
        zero_count = tx["zero_count"]
        reward = 0
        if zero_count >= Config()["MIN_ZERO_COUNT"]:
            reward = calculate_reward(zero_count, block["max_zero_count"])

        if reward > 0:
            nice_hash = utils.inverse_hash(binascii.hexlify(tx["tx_id"]).decode())
            print(f"{reward} MHIN rewarded for {nice_hash}")
        
        # Start transaction only if there's something to distribute
        mhin_store.start_transaction(tx["tx_id"], reward)

        # Collect inputs
        total_in = 0
        for vin in tx["vin"]:
            balance = mhin_store.pop_balance(vin["utxo_id"])
            total_in += balance

        # Distribute reward + inputs
        total_to_distribute = reward + total_in
        if total_to_distribute > 0:
            movements = generate_movements(tx, total_to_distribute, output_values)
            for movement in movements:
                if movement["quantity"] > 0:
                    mhin_store.add_balance(movement["utxo_id"], movement["quantity"])

        # End the current transaction
        mhin_store.end_transaction()

    # End the block
    mhin_store.end_block()
