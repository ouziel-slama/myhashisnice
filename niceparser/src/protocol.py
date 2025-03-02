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


def generate_movements(
    height, tx, position, source_utxo_id, quantity, output_values, zero_count
):
    movements = []
    distribution = calculate_distribution(quantity, output_values)
    # gather movements
    for out in tx["vout"]:
        if not out["is_op_return"] and distribution:
            movements.append(
                {
                    "height": height,
                    "txid": tx["tx_id"],
                    "position": position,
                    "utxo_id": out["utxo_id"],
                    "destination": out["n"],
                    "quantity": distribution.pop(0),
                    "source_utxo_id": source_utxo_id,
                    "address_hash": out["address_hash"],
                    "zero_count": zero_count,
                }
            )
    return movements


def process_block(block, mhin_store):
    height = block["height"]
    mhin_store.start_block(height, block["block_hash"])
    movement_count = 0
    block_movements = []
    txids = []

    for position, tx in enumerate(block["tx"]):
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
        tx_started = False
        if zero_count >= Config()["MIN_ZERO_COUNT"]:
            reward = calculate_reward(zero_count, block["max_zero_count"])

        tx_movements = 0
        if reward > 0:
            mhin_store.start_transaction(tx["tx_id"], reward)
            tx_started = True
            movements = generate_movements(
                height,
                tx,
                position,
                "reward",
                reward,
                output_values,
                zero_count
            )
            count = len(movements)
            movement_count += count
            tx_movements += count
            if count > 0:
                nice_hash = utils.inverse_hash(binascii.hexlify(tx["tx_id"]).decode())
                for movement in movements:
                    mhin_store.add_balance(movement["utxo_id"], movement["quantity"], "reward")
                    print(f'{movement["quantity"]} rewarded to {nice_hash}:{movement["destination"]}')
            block_movements += movements

        total_in = 0
        for vin in tx["vin"]:
            balance = mhin_store.pop_balance(vin['utxo_id'])
            total_in += balance

        if total_in > 0:
            # send movements
            if not tx_started:
                mhin_store.start_transaction(tx["tx_id"], 0)
            movements = generate_movements(
                height,
                tx,
                position,
                vin['utxo_id'],
                total_in,
                output_values,
                zero_count
            )
            count = len(movements)
            movement_count += count
            tx_movements += count
            for movement in movements:
                mhin_store.add_balance(movement["utxo_id"], movement["quantity"], "movement")
            block_movements += movements

        if tx_movements > 0:
            txids.append({
                "txid": tx["tx_id"],
                "height": height,
                "position": position,
                "zero_count": zero_count,
            })
    mhin_store.end_block()
    return block_movements, txids
