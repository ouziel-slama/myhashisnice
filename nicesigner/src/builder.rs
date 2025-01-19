use bitcoin::{
    absolute::LockTime, 
    blockdata::transaction::{Transaction, Version},
    key::Keypair, key::Secp256k1, script::PushBytesBuf, sighash::SighashCache, Amount,
    EcdsaSighashType, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness,
    consensus::serialize,
};

use hex;
use secp256k1::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use pyo3::prelude::*;

#[derive(Clone)]
struct Input {
    txid: String,
    vout: u32,
    value: u64,
    script_pubkey: String,
}

#[derive(Clone)]
struct Output {
    value: u64,
    script_pubkey: String,
}

pub fn create_op_return_script(message: &str) -> ScriptBuf {
    // Create a new PushBytesBuf and extend it with our data
    let mut push_bytes = PushBytesBuf::new();
    push_bytes
        .extend_from_slice(message.as_bytes())
        .expect("Message too long for push bytes");

    // Create a new OP_RETURN script with the push bytes
    ScriptBuf::new_op_return(&push_bytes)
}

fn build_unsigned_transaction(inputs: Vec<Input>, outputs: Vec<Output>, data: &str) -> Transaction {
    let mut unsigned_tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![],
        output: vec![],
    };

    for input in inputs {
        unsigned_tx.input.push(TxIn {
            previous_output: OutPoint {
                txid: input.txid.parse().unwrap(),
                vout: input.vout,
            },
            script_sig: ScriptBuf::default(),
            sequence: Sequence::ZERO,
            witness: Witness::default(),
        });
    }

    for output in outputs {
        unsigned_tx.output.push(TxOut {
            value: Amount::from_sat(output.value),
            script_pubkey: ScriptBuf::from(hex::decode(output.script_pubkey).unwrap()),
        });
    }

    unsigned_tx.output.push(TxOut {
        value: Amount::from_sat(0),
        script_pubkey: create_op_return_script(data),
    });

    unsigned_tx
}

fn sign_transaction(
    unsigned_tx: &mut Transaction,
    inputs: Vec<Input>,
    private_key: &str,
) -> Transaction {
    let secp = Secp256k1::new();
    let keypair = Keypair::from_seckey_str(&secp, private_key).expect("failed to create keypair");
    let secret_key = keypair.secret_key();

    let mut sighasher = SighashCache::new(unsigned_tx);

    for (input_index, input) in inputs.iter().enumerate() {
        let input_script_pubkey =
            ScriptBuf::from(hex::decode(input.script_pubkey.clone()).unwrap());
        let sighash = sighasher
            .p2wpkh_signature_hash(
                input_index,
                &input_script_pubkey,
                Amount::from_sat(input.value),
                EcdsaSighashType::All,
            )
            .expect("failed to create sighash");
        let msg = Message::from(sighash);
        let signature = secp.sign_ecdsa(&msg, &secret_key);

        let signature = bitcoin::ecdsa::Signature {
            signature,
            sighash_type: EcdsaSighashType::All,
        };
        let public_key = secret_key.public_key(&secp);
        *sighasher.witness_mut(input_index).unwrap() = Witness::p2wpkh(&signature, &public_key);
    }
    let signed_tx = sighasher.into_transaction();

    signed_tx.clone()
}


fn build_signed_transaction(
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    data: &str,
    private_key: &str,
) -> Transaction {
    let mut unsigned_tx = build_unsigned_transaction(inputs.clone(), outputs, data);
    sign_transaction(&mut unsigned_tx, inputs, private_key)
}

fn build_nice_signed_transaction(
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    private_key: &str,
    start: u64,
    target: usize,
    increment: usize,
    stop_signal: &Arc<AtomicBool>,
) -> Transaction {
    let prefix = "myhashisnice.com";
    let mut data = prefix.to_string();
    let mut count = start;
    let start_time = Instant::now();

    loop {
        if stop_signal.load(Ordering::SeqCst) {
            break;
        }

        let signed_tx =
            build_signed_transaction(inputs.clone(), outputs.clone(), &data, private_key);
        let txid = signed_tx.compute_txid();

        if txid.to_string().starts_with(&"0".repeat(target)) {
            let elapsed_time = start_time.elapsed();
            println!("Elapsed time: {:?}", elapsed_time);
            println!("Data: {}", data);
            return signed_tx;
        }

        data = format!("{} {}", prefix, count);
        count += increment as u64;
    }

    Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![],
        output: vec![],
    }
}

fn build_signed_transactions_parallel(
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    private_key: &str,
    first_thread: usize,
    num_threads: usize,
    total_threads: usize,
    target: usize,
) -> Transaction {
    let found_tx = Arc::new(Mutex::new(None));
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    let increment = total_threads;
    for start in first_thread..=num_threads {
        if stop_signal.load(Ordering::SeqCst) {
            break;
        }

        let inputs_clone = inputs.clone();
        let outputs_clone = outputs.clone();
        let private_key_clone = private_key.to_string();
        let found_tx_clone = Arc::clone(&found_tx);
        let stop_signal_clone = Arc::clone(&stop_signal);

        let handle = thread::spawn(move || {
            let signed_tx = build_nice_signed_transaction(
                inputs_clone,
                outputs_clone,
                &private_key_clone,
                start as u64,
                target,
                increment,
                &stop_signal_clone,
            );
            let txid = signed_tx.compute_txid();

            if txid.to_string().starts_with(&"0".repeat(target)) {
                let mut found = found_tx_clone.lock().unwrap();
                *found = Some(signed_tx);
                stop_signal_clone.store(true, Ordering::SeqCst);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let found = found_tx.lock().unwrap();
    found.clone().expect("No transaction found")
}


fn parse_inputs(inputs_set: &str) -> Vec<Input> {
    inputs_set.split(',')
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == 4 {
                Some(Input {
                    txid: parts[0].to_string(),
                    vout: parts[1].parse().unwrap(),
                    value: parts[2].parse().unwrap(),
                    script_pubkey: parts[3].to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}


fn parse_outputs(outputs_set: &str) -> Vec<Output> {
    outputs_set.split(',')
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == 2 {
                Some(Output {
                    value: parts[0].parse().unwrap(),
                    script_pubkey: parts[1].to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

#[pyfunction]
pub fn build_transaction(
    inputs_set: &str,
    output_set: &str,
    private_key: &str,
    first_thread: usize,
    num_threads: usize,
    total_threads: usize,
    target: usize
) -> PyResult<String> {
    let inputs = parse_inputs(inputs_set);
    let outputs = parse_outputs(output_set);
    let signed_tx = build_signed_transactions_parallel(
        inputs,
        outputs,
        private_key,
        first_thread,
        num_threads,
        total_threads,
        target
    );
    Ok(hex::encode(serialize(&signed_tx)))
}

