use bitcoin::{
    blockdata::transaction::{Transaction, Version},
    sighash::SighashCache,
    Amount,
    EcdsaSighashType,
    OutPoint,
    ScriptBuf,
    Sequence,
    TxIn,
    TxOut,
    Witness,
    Network,
    Address,
    consensus::serialize,
    key::CompressedPublicKey,
};

use hdwallet::{KeyChain, DefaultKeyChain, ExtendedPrivKey, ChainPath};
use bip39::Mnemonic;
use secp256k1::{Message, Secp256k1};
use hex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::fs;
use std::io::Write;
use std::time::Instant;
use pyo3::prelude::*;
use zerocopy::AsBytes;  // Import manquant

#[derive(Clone)]
struct Input {
    txid: String,
    vout: u32,
    value: u64,
    script_pubkey: String,
    derivation_path: String,
}

#[derive(Clone)]
struct Output {
    value: u64,
    script_pubkey: String,
}

struct CachedKeys {
    secp: Secp256k1<secp256k1::All>,
    keychain: DefaultKeyChain,
}

fn save_derivation_path(txid: &str, path: &str) -> std::io::Result<()> {
    let home = dirs::home_dir().expect("Could not find home directory");
    let mut dir_path = home;
    dir_path.push(".nicesigner");
    
    fs::create_dir_all(&dir_path)?;
    let mut file_path = dir_path;
    file_path.push(txid);
    fs::write(file_path, path)?;
    
    Ok(())
}

fn build_transactions_parallel(
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    mnemonic: &str,
    base_path: &str,
    first_thread: u32,
    num_threads: u32,
    total_threads: u32,
    target: usize,
    min_value: u64,
) -> (Transaction, String) {
    let found_result = Arc::new(Mutex::new(None));
    let stop_signal = Arc::new(AtomicBool::new(false));
    let total_attempts = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();
    let mut handles = vec![];

    let mnemonic = Arc::new(Mnemonic::parse_normalized(mnemonic).unwrap());
    let seed = mnemonic.to_seed("");
    let master = ExtendedPrivKey::with_seed(seed.as_bytes()).unwrap();
    
    let cached_keys = CachedKeys {
        secp: Secp256k1::new(),
        keychain: DefaultKeyChain::new(master),
    };

    for start in first_thread..num_threads {
        let inputs_clone = inputs.clone();
        let outputs_clone = outputs.clone();
        let base_path = base_path.to_string();
        let found_result = Arc::clone(&found_result);
        let stop_signal = Arc::clone(&stop_signal);
        let total_attempts = Arc::clone(&total_attempts);
        let start_time = start_time;
        let mnemonic_clone = Arc::clone(&mnemonic);

        let handle = std::thread::spawn(move || {
            let seed = mnemonic_clone.to_seed("");
            let master = ExtendedPrivKey::with_seed(seed.as_bytes()).unwrap();
            
            let cached_keys = CachedKeys {
                secp: Secp256k1::new(),
                keychain: DefaultKeyChain::new(master),
            };

            if stop_signal.load(Ordering::Relaxed) {
                return;
            }

            if let Some((tx, path)) = find_nice_transaction(
                &inputs_clone,
                &outputs_clone,
                &cached_keys,
                &base_path,
                start,
                target,
                total_threads,
                min_value,
                &stop_signal,
                &total_attempts,
                &start_time,
            ) {
                let mut found = found_result.lock().unwrap();
                *found = Some((tx, path));
                stop_signal.store(true, Ordering::Relaxed);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let found = found_result.lock().unwrap();
    match &*found {
        Some((tx, path)) => {
            // Signer la transaction finale
            let seed = mnemonic.to_seed("");
            let signed_tx = sign_transaction(tx.clone(), &inputs, seed.as_bytes().to_vec(), path);
            (signed_tx, path.clone())
        },
        None => panic!("Opération interrompue par l'utilisateur")
    }
}

fn find_nice_transaction(
    inputs: &[Input],
    outputs: &[Output],
    cached_keys: &CachedKeys,
    base_path: &str,
    start_index: u32,
    target: usize,
    increment: u32,
    min_value: u64,
    stop_signal: &Arc<AtomicBool>,
    total_attempts: &Arc<AtomicUsize>,
    start_time: &Instant,
) -> Option<(Transaction, String)> {
    println!("Starting thread with start index {} and increment {}", start_index, increment);
    let target_prefix = "0".repeat(target);
    let mut current_index = start_index;
    let mut attempts_count = 0u64;
    
    let mut tx_template = Transaction {
        version: Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: Vec::with_capacity(inputs.len()),
        output: Vec::with_capacity(outputs.len() + 1),
    };

    for input in inputs {
        tx_template.input.push(TxIn {
            previous_output: OutPoint {
                txid: input.txid.parse().unwrap(),
                vout: input.vout,
            },
            script_sig: ScriptBuf::default(),
            sequence: Sequence::ZERO,
            witness: Witness::default(),
        });
    }

    while !stop_signal.load(Ordering::Relaxed) {
        let output_path = format!("{}/{}", base_path, current_index);
        let chain_path = ChainPath::from(output_path.as_str());
        
        let (derived_key, _) = cached_keys.keychain.derive_private_key(chain_path).unwrap();
        let secp_pubkey = secp256k1::PublicKey::from_secret_key(&cached_keys.secp, &secp256k1::SecretKey::from_slice(derived_key.private_key.as_ref()).unwrap());
        let compressed_pubkey = CompressedPublicKey::from_slice(&secp_pubkey.serialize()).unwrap();
        let address = Address::p2wpkh(&compressed_pubkey, Network::Bitcoin);

        let mut tx = tx_template.clone();
        
        // Ajouter la sortie de vanité
        tx.output.push(TxOut {
            value: Amount::from_sat(min_value),
            script_pubkey: address.script_pubkey(),
        });

        // Ajouter les sorties supplémentaires
        for output in outputs {
            tx.output.push(TxOut {
                value: Amount::from_sat(output.value),
                script_pubkey: ScriptBuf::from(hex::decode(&output.script_pubkey).unwrap()),
            });
        }

        // Vérification du txid sans avoir besoin de signer
        let txid = tx.compute_txid().to_string();
        if txid.starts_with(&target_prefix) && txid.chars().nth(target_prefix.len()) != Some('0') {
            if let Err(e) = save_derivation_path(&txid, &output_path) {
                eprintln!("Failed to save derivation path: {}", e);
            }
            let total = total_attempts.load(Ordering::Relaxed);
            println!("\nFound matching transaction! Total attempts: {}", total);
            return Some((tx, output_path));
        }

        current_index += increment;
        attempts_count += 1;
        
        if attempts_count % 10000 == 0 {
            let new_total = total_attempts.fetch_add(10000, Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let speed = (new_total + 10000) as f64 / elapsed.as_secs_f64();
            print!("\rTotal attempts: {} - Elapsed: {:.2}s - Speed: {:.0} txid/s     ", 
                new_total + 10000,
                elapsed.as_secs_f64(),
                speed);
            std::io::stdout().flush().unwrap();
        }
    }

    None
}

fn sign_transaction(
    mut unsigned_tx: Transaction,
    inputs: &[Input],
    seed: Vec<u8>,
    vanity_path: &str,
) -> Transaction {
    println!("Signing final transaction...");
    
    // Créer les objets nécessaires pour la signature
    let master = ExtendedPrivKey::with_seed(&seed).unwrap();
    let keychain = DefaultKeyChain::new(master);
    let secp = Secp256k1::new();
    
    let mut sighashes = Vec::with_capacity(inputs.len());
    {
        let mut sighasher = SighashCache::new(&unsigned_tx);
        for (input_index, input) in inputs.iter().enumerate() {
            let input_script_pubkey = ScriptBuf::from(hex::decode(&input.script_pubkey).unwrap());
            let sighash = sighasher
                .p2wpkh_signature_hash(
                    input_index,
                    &input_script_pubkey,
                    Amount::from_sat(input.value),
                    EcdsaSighashType::All,
                )
                .expect("failed to create sighash");
            sighashes.push(sighash);
        }
    }

    for (input_index, (sighash, input)) in sighashes.into_iter().zip(inputs.iter()).enumerate() {
        let chain_path = ChainPath::from(input.derivation_path.as_str());
        let (derived_key, _) = keychain.derive_private_key(chain_path).unwrap();
        let secp_secret_key = secp256k1::SecretKey::from_slice(derived_key.private_key.as_ref()).unwrap();
        let secp_pubkey = secp256k1::PublicKey::from_secret_key(&secp, &secp_secret_key);

        let msg = Message::from(sighash);
        let signature = secp.sign_ecdsa(&msg, &secp_secret_key);

        let signature = bitcoin::ecdsa::Signature {
            signature,
            sighash_type: EcdsaSighashType::All,
        };
        
        unsigned_tx.input[input_index].witness = Witness::p2wpkh(&signature, &secp_pubkey);
    }
    
    // Signer également la sortie de vanité si nécessaire
    // Note: Ce n'est généralement pas nécessaire pour le fonctionnement,
    // mais ajouté pour être complet si vous avez besoin d'utiliser cette adresse plus tard
    let chain_path = ChainPath::from(vanity_path);
    let (derived_key, _) = keychain.derive_private_key(chain_path).unwrap();
    let _vanity_secret_key = secp256k1::SecretKey::from_slice(derived_key.private_key.as_ref()).unwrap();
    // La clé privée de la sortie de vanité est disponible ici si vous en avez besoin
    
    println!("Transaction signed successfully");
    unsigned_tx
}

fn parse_inputs(inputs_set: &str) -> Vec<Input> {
    if inputs_set.is_empty() {
        return vec![];
    }
    
    inputs_set.split(',')
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == 5 {
                Some(Input {
                    txid: parts[0].to_string(),
                    vout: parts[1].parse().unwrap(),
                    value: parts[2].parse().unwrap(),
                    script_pubkey: parts[3].to_string(),
                    derivation_path: parts[4].to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn parse_outputs(outputs_set: &str) -> Vec<Output> {
    if outputs_set.is_empty() {
        return vec![];
    }
    
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
    mnemonic: &str,
    base_path: &str,
    first_thread: u32,
    num_threads: u32,
    total_threads: u32,
    target: usize,
    min_value: u64,
) -> PyResult<(String, String)> {
    let inputs = parse_inputs(inputs_set);
    let outputs = parse_outputs(output_set);
    
    let (signed_tx, path) = build_transactions_parallel(
        inputs,
        outputs,
        mnemonic,
        base_path,
        first_thread,
        num_threads,
        total_threads,
        target,
        min_value
    );
    
    Ok((hex::encode(serialize(&signed_tx)), path))
}