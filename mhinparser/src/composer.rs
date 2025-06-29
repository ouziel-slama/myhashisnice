use crate::config::MhinConfig;
use crate::psbt::Psbt;
use crate::rpc::make_rpc_call;
use crate::mempool::MempoolClient;
use crate::block::generate_utxo_id;
use crate::store::utils::inverse_hash;
use crate::store::MhinStore;

use bitcoin::{
    script::{Builder, Script},
    opcodes::all::OP_RETURN,
    Address, Amount, OutPoint, TxOut, Txid,
    psbt::{Psbt as BitcoinPsbt, Input as PsbtInput, Output as PsbtOutput},
    Transaction, TxIn,
};
use serde_json::json;
use std::str::FromStr;
use std::collections::{BTreeMap, HashMap};
use hex::FromHex;
use std::sync::Arc;

#[derive(Debug)]
pub enum ComposerError {
    InvalidTxid(String),
    InvalidAddress(String),
    RpcError(String),
    CborError(String),
    PsbtError(String),
    InsufficientFunds(String),
    ExternalApi(String),
}

impl std::fmt::Display for ComposerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComposerError::InvalidTxid(msg) => write!(f, "Invalid txid: {}", msg),
            ComposerError::InvalidAddress(msg) => write!(f, "Invalid address: {}", msg),
            ComposerError::RpcError(msg) => write!(f, "RPC error: {}", msg),
            ComposerError::CborError(msg) => write!(f, "CBOR error: {}", msg),
            ComposerError::PsbtError(msg) => write!(f, "PSBT error: {}", msg),
            ComposerError::InsufficientFunds(msg) => write!(f, "Insufficient funds: {}", msg),
            ComposerError::ExternalApi(msg) => write!(f, "External API error: {}", msg),
        }
    }
}

impl std::error::Error for ComposerError {}

#[derive(Debug, Clone)]
pub struct TxInput {
    pub txid: String,
    pub vout: u32,
}

#[derive(Debug, Clone)]
pub struct TxOutput {
    pub address: String,
    pub amount: u64,
}

#[derive(serde::Deserialize)]
struct UtxoInfo {
    value: f64,
    #[serde(rename = "scriptPubKey")]
    script_pub_key: ScriptPubKey,
}

#[derive(serde::Deserialize)]
struct ScriptPubKey {
    hex: String,
    #[serde(rename = "type")]
    script_type: Option<String>,
}

#[derive(Debug)]
struct UtxoWithBalances {
    txid: String,
    vout: u32,
    btc_amount: u64, // in satoshis
    mhin_amount: u64,
}

fn estimate_vsize(num_inputs: usize, num_outputs: usize) -> usize {
    // Approximation for P2WPKH inputs/outputs:
    // overhead ~ 10 vbytes
    // each input ~ 68 vbytes  
    // each output ~ 31 vbytes
    // OP_RETURN output ~ 43 vbytes (for MHIN data)
    10 + num_inputs * 68 + num_outputs * 31 + 43
}

pub async fn compose_transaction(
    config: &MhinConfig,
    store: Arc<MhinStore>,
    source_address: &String,
    mhin_outputs: &HashMap<String, u64>, // address -> MHIN amount
    fee_rate: u64, // sat/vbyte
) -> Result<Psbt, ComposerError> {
    let dust_amount = 294u64; // Standard dust limit for P2WPKH (legacy P2PKH is 546)

    // 1. Get UTXOs for source address using mempool client
    let mempool_client = MempoolClient::new();
    let utxos = mempool_client.get_address_utxos(source_address).await
        .map_err(|e| ComposerError::ExternalApi(format!("Failed to fetch UTXOs: {}", e)))?;

    // 2. Get MHIN balances for each UTXO from the source
    let mut available_utxos = Vec::new();
    for utxo in utxos {
        let txid_bytes = Vec::from_hex(inverse_hash(&utxo.txid))
            .map_err(|e| ComposerError::ExternalApi(format!("Invalid txid format: {}", e)))?;

        let utxo_id = generate_utxo_id(&txid_bytes, utxo.vout as u64);
        let mhin_amount = store.get_balance(utxo_id).unwrap_or(0);
        
        available_utxos.push(UtxoWithBalances {
            txid: utxo.txid,
            vout: utxo.vout,
            btc_amount: utxo.value,
            mhin_amount,
        });
    }
    available_utxos.sort_by(|a, b| b.mhin_amount.cmp(&a.mhin_amount)); // Sort by MHIN amount descending

    // 3. Select UTXOs 
    let total_mhin_needed: u64 = mhin_outputs.values().sum();

    let mut inputs = Vec::new();
    let mut btc_in = 0u64;
    let mut mhin_in = 0u64;
    let mut btc_out = 0u64;
    let mut fee = 0u64;
    
    for utxo in available_utxos {
        inputs.push(TxInput {
            txid: utxo.txid,
            vout: utxo.vout,
        });
        btc_in += utxo.btc_amount;
        mhin_in += utxo.mhin_amount;
        
        if mhin_in >= total_mhin_needed {
            btc_out = 0u64;
            let mut mhin_distribution = Vec::new();
            
            // Build outputs
            let mut outputs = Vec::new();
            for (address, mhin_amount) in mhin_outputs {
                outputs.push(TxOutput {
                    address: address.clone(),
                    amount: dust_amount,
                });
                btc_out += dust_amount;
                mhin_distribution.push(*mhin_amount);
            }

            // Add change output if needed
            let mut change_amount = 0u64;
            let with_mhin_change = mhin_in > total_mhin_needed;
            let with_btc_change = btc_in > btc_out + dust_amount;
            if with_mhin_change || with_btc_change {
                change_amount = if with_btc_change {
                    btc_in - btc_out
                } else {
                    dust_amount
                };
                outputs.push(TxOutput {
                    address: source_address.clone(),
                    amount: change_amount,
                });
                btc_out += change_amount;
                mhin_distribution.push(mhin_in - total_mhin_needed);
            }

            // Calculate fee
            let vsize = estimate_vsize(inputs.len(), outputs.len());
            fee = (vsize as u64) * fee_rate;
      
            if change_amount > 0 && fee > change_amount + dust_amount {
                outputs.last_mut().unwrap().amount -= fee;
                btc_out -= fee;
            }

            if btc_in < btc_out + fee {
                continue;
            }

            return compose_psbt(config, &inputs, &outputs, &mhin_distribution).await;
        }
    }

    Err(ComposerError::InsufficientFunds(format!("Insufficient funds: {} < {}", btc_in, btc_out + fee)))
}

/// Compose a PSBT transaction with MHIN OP_RETURN data
/// This function mimics Bitcoin Core's createrawtransaction + createpsbt functionality
pub async fn compose_psbt(
    config: &MhinConfig,
    inputs: &[TxInput],
    outputs: &[TxOutput],
    mhin_distribution: &[u64],
) -> Result<Psbt, ComposerError> {
    // Validate inputs
    if inputs.is_empty() {
        return Err(ComposerError::RpcError("No inputs provided".to_string()));
    }
    if outputs.is_empty() {
        return Err(ComposerError::RpcError("No outputs provided".to_string()));
    }

    // 1. Create transaction inputs and collect UTXO info
    let mut tx_inputs = Vec::new();
    let mut psbt_inputs = Vec::new();
    
    for input in inputs {
        let txid = Txid::from_str(&input.txid)
            .map_err(|e| ComposerError::InvalidTxid(format!("{}: {}", input.txid, e)))?;
        
        let outpoint = OutPoint {
            txid,
            vout: input.vout,
        };
        
        // Get UTXO info from RPC
        let utxo_info = get_utxo_info(config, &input.txid, input.vout).await?;
        
        // Create transaction input
        let tx_in = TxIn {
            previous_output: outpoint,
            script_sig: Script::new().into(),
            sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: bitcoin::Witness::new(),
        };
        tx_inputs.push(tx_in);
        
        // Create PSBT input with appropriate UTXO reference
        let script_bytes = hex::decode(&utxo_info.script_pub_key.hex)
            .map_err(|e| ComposerError::RpcError(format!("Invalid script hex: {}", e)))?;
        let script_pubkey = Script::from_bytes(&script_bytes);
        
        let value = Amount::from_btc(utxo_info.value)
            .map_err(|e| ComposerError::RpcError(format!("Invalid value: {}", e)))?;
            
        let utxo = TxOut {
            value,
            script_pubkey: script_pubkey.into(),
        };
        
        let mut psbt_input = PsbtInput::default();
        
        // Use witness_utxo for segwit transactions, non_witness_utxo for legacy
        match utxo_info.script_pub_key.script_type.as_deref() {
            Some("witness_v0_keyhash") | Some("witness_v0_scripthash") | Some("witness_v1_taproot") => {
                psbt_input.witness_utxo = Some(utxo);
            }
            _ => {
                // For legacy transactions, we need the full previous transaction
                // For now, we'll use witness_utxo as fallback (most wallets support this)
                psbt_input.witness_utxo = Some(utxo);
            }
        }
        
        psbt_inputs.push(psbt_input);
    }
    
    // 2. Create transaction outputs
    let mut tx_outputs = Vec::new();
    let mut psbt_outputs = Vec::new();
    
    for output in outputs {
        let address = Address::from_str(&output.address)
            .map_err(|e| ComposerError::InvalidAddress(format!("{}: {}", output.address, e)))?
            .require_network(config.network)
            .map_err(|e| ComposerError::InvalidAddress(format!("Address network mismatch: {}", e)))?;
        
        let tx_out = TxOut {
            value: Amount::from_sat(output.amount),
            script_pubkey: address.script_pubkey(),
        };
        tx_outputs.push(tx_out);
        
        // Empty PSBT output (no additional data needed for outputs)
        psbt_outputs.push(PsbtOutput::default());
    }
    
    // 3. Add OP_RETURN output with MHIN data (if distribution is not empty)
    if !mhin_distribution.is_empty() {
        let op_return_output = create_mhin_op_return(mhin_distribution)?;
        tx_outputs.push(op_return_output);
        psbt_outputs.push(PsbtOutput::default());
    }
    
    // 4. Create the transaction (similar to createrawtransaction)
    let transaction = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: tx_inputs,
        output: tx_outputs,
    };
    
    // 5. Create the PSBT (similar to createpsbt)
    let psbt = BitcoinPsbt {
        unsigned_tx: transaction,
        version: 0,
        xpub: BTreeMap::new(),
        proprietary: BTreeMap::new(),
        unknown: BTreeMap::new(),
        inputs: psbt_inputs,
        outputs: psbt_outputs,
    };
    
    // 6. Convert to our Psbt type
    Ok(Psbt::from(psbt))
}

/// Create OP_RETURN output with MHIN distribution data
fn create_mhin_op_return(mhin_distribution: &[u64]) -> Result<TxOut, ComposerError> {
    // Encode distribution as CBOR
    let mut cbor_data = Vec::new();
    ciborium::into_writer(mhin_distribution, &mut cbor_data)
        .map_err(|e| ComposerError::CborError(format!("Failed to encode CBOR: {}", e)))?;
    
    // Create data with MHIN prefix
    let mut op_return_data = b"MHIN".to_vec();
    op_return_data.extend_from_slice(&cbor_data);
    
    // Bitcoin has a limit on OP_RETURN size (80 bytes by default)
    if op_return_data.len() > 80 {
        return Err(ComposerError::CborError(
            format!("OP_RETURN data too large: {} bytes (max 80)", op_return_data.len())
        ));
    }
    
    // Create OP_RETURN script
    use bitcoin::script::PushBytesBuf;
    let push_bytes = PushBytesBuf::try_from(op_return_data)
        .map_err(|e| ComposerError::CborError(format!("Failed to create push bytes: {}", e)))?;
    
    let script = Builder::new()
        .push_opcode(OP_RETURN)
        .push_slice(push_bytes)
        .into_script();
    
    Ok(TxOut {
        value: Amount::ZERO,
        script_pubkey: script,
    })
}

/// Get UTXO information from Bitcoin RPC
async fn get_utxo_info(
    config: &MhinConfig, 
    txid: &str, 
    vout: u32
) -> Result<UtxoInfo, ComposerError> {
    let response = make_rpc_call(
        &config.rpc_url,
        "gettxout",
        json!([txid, vout, true]) // true for include_mempool
    ).await.ok_or_else(|| ComposerError::RpcError("Failed to call gettxout".to_string()))?;
    
    if response.is_null() {
        return Err(ComposerError::RpcError(
            format!("UTXO {}:{} not found or already spent", txid, vout)
        ));
    }
    
    serde_json::from_value(response)
        .map_err(|e| ComposerError::RpcError(format!("Failed to parse gettxout response: {}", e)))
}