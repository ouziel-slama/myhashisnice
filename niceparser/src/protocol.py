import binascii
from decimal import Decimal as D

from config import Config
from nicefetcher import utils


def calculate_reward(zero_count, max_zero_count):
    if zero_count < Config()["MIN_ZERO_COUNT"]:
        return 0
    reward = D(Config()["MAX_REWARD"]) / D(16 ** (max_zero_count - zero_count))
    return int(reward)


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


def generate_movements(tx, quantity, output_values):
    movements = []
    distribution = calculate_distribution(quantity, output_values)
    # gather movements
    for out in tx["vout"]:
        if not out["is_op_return"] and distribution:
            movements.append(
                {
                    "utxo_id": out["utxo_id"],
                    "quantity": distribution.pop(0),
                }
            )
    return movements


def process_block(block, mhin_store):
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
        # don't distribute to the last output if more than one
        if len(output_values) > 1:
            output_values.pop()

        # reward movements
        zero_count = tx["zero_count"]
        reward = 0
        if zero_count >= Config()["MIN_ZERO_COUNT"]:
            reward = calculate_reward(zero_count, block["max_zero_count"])

        if reward > 0:
            nice_hash = utils.inverse_hash(binascii.hexlify(tx["tx_id"]).decode())
            print(f"{reward} MHIN rewarded to {nice_hash}")

        total_in = 0
        for vin in tx["vin"]:
            balance = mhin_store.pop_balance(vin["utxo_id"])
            total_in += balance

        total_to_distribute = reward + total_in
        if total_to_distribute > 0:
            mhin_store.start_transaction(tx["tx_id"], reward)
            movements = generate_movements(tx, total_to_distribute, output_values)
            for movement in movements:
                mhin_store.add_balance(movement["utxo_id"], movement["quantity"])

    mhin_store.end_block()
