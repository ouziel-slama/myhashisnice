use crate::block::{MhinBlock, MhinMovement, MhinMovementType};
use crate::config::MhinConfig;
use crate::store::MhinStore;

use std::collections::HashMap;

use log::debug;

/*pub fn get_zero_count(txid: &[u8]) -> u64 {
    let mut count = 0;
    for &byte in txid.iter().rev() {
        if byte == 0 {
            count += 2;
        } else if byte < 0x10 {
            count += 1;
            break;
        } else {
            break;
        }
    }
    count
}*/

pub fn get_zero_count(txid: &[u8]) -> u64 {
    let len = txid.len();
    if len == 0 {
        return 0;
    }

    let mut count = 0;
    let ptr = txid.as_ptr();
    let mut offset = len as isize;

    unsafe {
        loop {
            offset -= 1;
            let byte = *ptr.offset(offset);

            if byte != 0 {
                count += (byte < 0x10) as u64;
                break;
            }

            count += 2;

            if offset == 0 {
                break;
            }
        }
    }
    count
}

pub fn calculate_reward(zero_count: u64, max_zero_count: u64, config: &MhinConfig) -> u64 {
    if zero_count < config.min_zero_count {
        return 0;
    }

    let power = max_zero_count - zero_count;
    let max_reward = config.max_reward as f64;
    let divisor = (16f64).powi(power as i32);

    (max_reward / divisor) as u64
}

// Helper function to calculate distribution proportionally
fn calculate_distribution(value: u64, output_values: &[u64]) -> Vec<u64> {
    let total_output: u64 = output_values.iter().sum();

    if total_output == 0 {
        return vec![0; output_values.len()];
    }

    // Calculate proportional distribution
    let mut distribution = Vec::with_capacity(output_values.len());
    let mut total_distributed = 0u64;

    for &output_value in output_values {
        // Use f64 for the division to avoid rounding issues
        let proportion = output_value as f64 / total_output as f64;
        let amount = (value as f64 * proportion).floor() as u64;
        distribution.push(amount);
        total_distributed += amount;
    }

    // Distribute any remaining value to the first output
    if total_distributed < value && !distribution.is_empty() {
        distribution[0] += value - total_distributed;
    }

    distribution
}

pub fn process_block(block: &MhinBlock, mhin_store: &MhinStore) {
    debug!("Processing block {}", block.height);

    // Create processed block with movements
    let mut movements = Vec::new();

    // Track balances within this block
    let mut block_balances = HashMap::new();

    // Process each transaction
    for tx in &block.transactions {
        // Get valid outputs
        let mut valid_outputs = tx.outputs.clone();

        if valid_outputs.is_empty() {
            continue;
        }

        // Don't distribute to the last output if there are more than one
        if valid_outputs.len() > 1 {
            valid_outputs.pop();
        }

        // Get transaction reward
        let transaction_reward = tx.reward;

        // Process inputs and calculate total input value
        let mut total_in = 0u64;
        for &utxo_id in &tx.inputs {
            // Check if we have the balance already in this block
            let balance = block_balances
                .get(&utxo_id)
                .copied()
                .or_else(|| mhin_store.get_balance(utxo_id));

            if let Some(balance) = balance {
                if balance > 0 {
                    // Add a POP movement
                    movements.push(MhinMovement {
                        movement_type: MhinMovementType::Pop,
                        utxo_id,
                        value: balance,
                    });
                    total_in += balance;
                }
            }
        }

        // Distribute reward + inputs
        let total_to_distribute = transaction_reward + total_in;
        if total_to_distribute > 0 {
            // Get output values for distribution calculation
            let output_values: Vec<u64> = valid_outputs.iter().map(|out| out.value).collect();

            // Calculate distribution of values
            let distributions = calculate_distribution(total_to_distribute, &output_values);

            // Apply distributions to outputs
            for (i, output) in valid_outputs.iter().enumerate() {
                block_balances.insert(output.utxo_id, distributions[i]);

                // Add an ADD movement
                movements.push(MhinMovement {
                    movement_type: MhinMovementType::Add,
                    utxo_id: output.utxo_id,
                    value: distributions[i],
                });
            }
        }
    }

    // Add block to MHINStore with movements
    mhin_store.add_block(&block, &movements);
}
