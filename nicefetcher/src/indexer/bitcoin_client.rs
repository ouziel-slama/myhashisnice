use std::{collections::HashMap, sync::Arc, thread::JoinHandle};

use bitcoin::{
    hashes::Hash,
    Address, Block, BlockHash,
    PubkeyHash, 
    ScriptHash, 
    WitnessProgram,
    Network,
    address::NetworkChecked,
};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use std::cmp::max;
use xxhash_rust::xxh3::xxh3_64;
use std::str::FromStr;
use std::mem;

use super::{
    block::{
        Block as CrateBlock, ToBlock, Transaction, Vin, Vout,
    },
    config::{Config, Mode},
    stopper::Stopper,
    types::{
        entry::{
            BlockAtHeightHasHash, BlockAtHeightSpentOutputInTx,
            ScriptHashHasOutputsInBlockAtHeight, ToEntry, TxInBlockAtHeight, TxidVoutPrefix,
            WritableEntry,
        },
        error::Error,
        pipeline::{BlockHasEntries, BlockHasOutputs, BlockHasPrevBlockHash},
    },
    workers::new_worker_pool,
};

impl BlockHasEntries for Block {
    fn get_entries(&self, mode: Mode, height: u32) -> Vec<Box<dyn ToEntry>> {
        let hash = self.block_hash().as_byte_array().to_owned();
        let mut entries: Vec<Box<dyn ToEntry>> =
            vec![Box::new(WritableEntry::new(BlockAtHeightHasHash {
                height,
                hash,
            }))];
        if mode == Mode::Fetcher {
            return entries;
        }
        let mut script_hashes = HashMap::new();
        for tx in self.txdata.iter() {
            let entry = TxInBlockAtHeight {
                txid: tx.compute_txid().to_byte_array(),
                height,
            };
            entries.push(Box::new(WritableEntry::new(entry)));
            for i in tx.input.iter() {
                let entry = BlockAtHeightSpentOutputInTx {
                    txid: i.previous_output.txid.to_byte_array(),
                    vout: i.previous_output.vout,
                    height,
                };
                entries.push(Box::new(WritableEntry::new(entry)));
            }
            for o in tx.output.iter() {
                let script_hash = o.script_pubkey.script_hash().as_byte_array().to_owned();
                script_hashes.entry(script_hash).or_insert_with(|| {
                    let entry = ScriptHashHasOutputsInBlockAtHeight {
                        script_hash,
                        height,
                    };
                    entries.push(Box::new(WritableEntry::new(entry)));
                });
            }
        }
        entries
    }
}


fn get_zero_count(txid: &str) -> u32 {
    let mut count = 0;
    for char in txid.chars() {
        if char == '0' {
            count += 1;
        } else {
            break;
        }
    }
    count
}


/// Pack an Address into a compact Vec<u8>
pub fn pack(address: &Address<NetworkChecked>) -> Option<Vec<u8>> {
    match (
        address.pubkey_hash(),
        address.script_hash(),
        address.witness_program()
    ) {
        (Some(pubkey_hash), _, _) => {
            let mut packed = Vec::with_capacity(21);
            packed.push(0x01);  // P2PKH prefix
            packed.extend_from_slice(pubkey_hash.as_byte_array());
            Some(packed)
        },
        (_, Some(script_hash), _) => {
            let mut packed = Vec::with_capacity(21);
            packed.push(0x02);  // P2SH prefix
            packed.extend_from_slice(script_hash.as_byte_array());
            Some(packed)
        },
        (_, _, Some(witness_program)) => {
            let mut packed = Vec::with_capacity(22);
            packed.push(0x03);  // Witness prefix
            packed.push(witness_program.version() as u8);
            // Convert PushBytes to &[u8]
            packed.extend_from_slice(witness_program.program().as_bytes());
            Some(packed)
        },
        _ => None
    }
}

/// Pack a Bitcoin address string into a compact Vec<u8>
pub fn pack_address_string(address: &str, network: Network) -> Option<Vec<u8>> {
    Address::from_str(address)
        .ok()
        .and_then(|addr| addr.require_network(network).ok())
        .and_then(|addr| pack(&addr))
}

/// Unpack a Vec<u8> back into an Address
pub fn unpack(packed: &[u8], network: Network) -> Option<Address<NetworkChecked>> {
    if packed.is_empty() {
        return None;
    }

    match packed[0] {
        0x01 if packed.len() == 21 => {
            let pubkey_hash = PubkeyHash::from_slice(&packed[1..]).ok()?;
            Some(Address::p2pkh(pubkey_hash, network))
        },
        0x02 if packed.len() == 21 => {
            let script_hash = ScriptHash::from_slice(&packed[1..]).ok()?;
            Some(Address::p2sh_from_hash(script_hash, network))
        },
        0x03 if packed.len() >= 22 => {
            let version = packed[1];
            let program = &packed[2..];
            let witness_program = WitnessProgram::new(
                version.try_into().ok()?, 
                program
            ).ok()?;
            Some(Address::from_witness_program(witness_program, network))
        },
        _ => None
    }
}

pub fn hash_string_and_number(input_bytes: &Vec<u8>, input_num: u64) -> Vec<u8> {
    // Convertir le u64 en bytes
    let num_bytes = input_num.to_le_bytes();
    
    // Créer un vecteur pour stocker la concaténation
    let mut combined = Vec::with_capacity(input_bytes.len() + mem::size_of::<u64>());
    
    // Ajouter les bytes de l'entrée
    combined.extend_from_slice(&input_bytes);
    
    // Ajouter les bytes du nombre
    combined.extend_from_slice(&num_bytes);
    
    // Calculer le hash
    let hash = xxh3_64(&combined);
    
    // Convertir le hash en Vec<u8>
    hash.to_le_bytes().to_vec()
}


pub fn hash_string(input_string: &str) -> Vec<u8> {
    // Convertir le string en bytes
    let input_bytes = input_string.as_bytes();
    
    // Calculer le hash
    let hash = xxh3_64(input_bytes);
    
    // Convertir le hash en Vec<u8>
    hash.to_le_bytes().to_vec()
}

pub fn parse_transaction(
    tx: &bitcoin::blockdata::transaction::Transaction,
    config: &Config,
    height: u32,
) -> Transaction {
    let mut vins = Vec::new();
    for vin in tx.input.iter() {
        let hash = vin.previous_output.txid.as_byte_array().to_vec();
        let utxo_id = hash_string_and_number(
            &hash,
            vin.previous_output.vout as u64
        );
        vins.push(Vin {
            hash,
            n: vin.previous_output.vout,
            utxo_id,
        })
    }
    let mut vouts = Vec::new();
    let tx_id = tx.compute_txid();
    let tx_id_bytes = tx_id.as_byte_array().to_vec();
    let tx_id = tx_id.to_string();
    for (i, vout) in tx.output.iter().enumerate() {

        let address_hash = match Address::from_script(
            &vout.script_pubkey,
            &config.bitcoin_network()
        ) {
            Ok(addr) => {
                let addr_string = addr.to_string();
                hash_string(&addr_string)
            },
            Err(_) => vec![0x00], // ou un vecteur vide si aucune adresse valide
        };
        vouts.push(Vout {
            value: vout.value.to_sat(),
            address_hash: address_hash,
            is_op_return: vout.script_pubkey.is_op_return(),
            n: i as u64,
            utxo_id: hash_string_and_number(&tx_id_bytes, i as u64),
        });
    }
    
    
    Transaction {
        coinbase: tx.is_coinbase(),
        tx_id: tx_id_bytes,
        vin: vins,
        vout: vouts,
        zero_count: get_zero_count(&tx_id),
    }
}

impl ToBlock for Block {
    fn to_block(&self, config: Config, height: u32) -> CrateBlock {
        let mut transactions = Vec::new();
        let mut max_zero_count = 0;
        for tx in self.txdata.iter() {
            let tx = parse_transaction(tx, &config, height);
            max_zero_count = max(max_zero_count, tx.zero_count);
            transactions.push(tx);
        }
        CrateBlock {
            height,
            hash_prev: self.header.prev_blockhash.to_string(),
            block_time: self.header.time,
            block_hash: self.block_hash().to_string(),
            transaction_count: self.txdata.len(),
            transactions,
            max_zero_count: max_zero_count
        }
    }
}

pub fn parse_block(
    block: Block,
    config: &Config,
    height: u32,
) -> Result<CrateBlock, Error> {
    let mut max_zero_count = 0;
    let mut transactions = Vec::new();
    for tx in block.txdata.iter() {
        let tx = parse_transaction(tx, config, height);
        max_zero_count = max(max_zero_count, tx.zero_count);
        transactions.push(tx);
    }
    Ok(CrateBlock {
        height,
        hash_prev: block.header.prev_blockhash.to_string(),
        block_time: block.header.time,
        block_hash: block.block_hash().to_string(),
        transaction_count: block.txdata.len(),
        transactions,
        max_zero_count: max_zero_count,
    })
}

impl BlockHasPrevBlockHash for Block {
    fn get_prev_block_hash(&self) -> &BlockHash {
        &self.header.prev_blockhash
    }
}

impl BlockHasOutputs for Block {
    fn get_script_hash_outputs(&self, script_hash: [u8; 20]) -> Vec<(TxidVoutPrefix, u64)> {
        let mut outputs = Vec::new();
        for tx in self.txdata.iter() {
            for (i, o) in tx.output.iter().enumerate() {
                if script_hash == o.script_pubkey.script_hash().as_byte_array().as_ref() {
                    outputs.push((
                        TxidVoutPrefix {
                            txid: tx.compute_txid().to_byte_array(),
                            vout: i as u32,
                        },
                        o.value.to_sat(),
                    ));
                }
            }
        }
        outputs
    }
}

pub trait BitcoinRpc<B>: Send + Clone + 'static {
    fn get_block_hash(&self, height: u32) -> Result<BlockHash, Error>;
    fn get_block(&self, hash: &BlockHash) -> Result<Box<B>, Error>;
    fn get_blockchain_height(&self) -> Result<u32, Error>;
}

struct GetBlockHash {
    height: u32,
    sender: Sender<Result<BlockHash, Error>>,
}

struct GetBlock {
    hash: BlockHash,
    sender: Sender<Result<Box<Block>, Error>>,
}

struct GetBlockchainHeight {
    sender: Sender<Result<u32, Error>>,
}

type Channel<T> = (Sender<T>, Receiver<T>);

#[derive(Clone)]
struct Channels {
    get_block_hash: Channel<GetBlockHash>,
    get_block: Channel<GetBlock>,
    get_blockchain_height: Channel<GetBlockchainHeight>,
}

impl Channels {
    fn new(n: usize) -> Self {
        Channels {
            get_block_hash: bounded(n),
            get_block: bounded(n),
            get_blockchain_height: bounded(n),
        }
    }
}

#[derive(Clone)]
pub struct BitcoinClient {
    n: usize,
    config: Config,
    stopper: Stopper,
    channels: Channels,
}

impl BitcoinClient {
    pub fn new(config: &Config, stopper: Stopper, n: usize) -> Result<Self, Error> {
        Ok(BitcoinClient {
            n,
            config: config.clone(),
            stopper,
            channels: Channels::new(n),
        })
    }

    pub fn start(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        let (_tx, _rx) = unbounded();
        let client = BitcoinClientInner::new(&self.config)?;
        new_worker_pool(
            "BitcoinClient".into(),
            self.n,
            _rx,
            _tx,
            self.stopper.clone(),
            Self::worker(client, self.channels.clone()),
        )
    }

    fn worker(
        client: BitcoinClientInner,
        channels: Channels,
    ) -> impl Fn(Receiver<()>, Sender<()>, Stopper) -> Result<(), Error> + Clone {
        move |_, _, stopper| loop {
            let (_, done) = stopper.subscribe()?;
            select! {
              recv(done) -> _ => {
                return Ok(())
              },
              recv(channels.get_block_hash.1) -> msg => {
                if let Ok(GetBlockHash {height, sender}) = msg {
                  sender.send(client.get_block_hash(height))?;
                }
              },
              recv(channels.get_block.1) -> msg => {
                if let Ok(GetBlock {hash, sender}) = msg {
                  sender.send(client.get_block(&hash))?;
                }
              },
              recv(channels.get_blockchain_height.1) -> msg => {
                if let Ok(GetBlockchainHeight {sender}) = msg {
                  sender.send(client.get_blockchain_height())?;
                }
              }
            }
        }
    }
}

impl BitcoinRpc<Block> for BitcoinClient {
    fn get_block_hash(&self, height: u32) -> Result<BlockHash, Error> {
        let (tx, rx) = bounded(1);
        self.channels
            .get_block_hash
            .0
            .send(GetBlockHash { height, sender: tx })?;
        let (id, done) = self.stopper.subscribe()?;
        select! {
            recv(done) -> _ => Err(Error::Stopped),
            recv(rx) -> result => {
                self.stopper.unsubscribe(id)?;
                result?
            }
        }
    }

    fn get_block(&self, hash: &BlockHash) -> Result<Box<Block>, Error> {
        let (tx, rx) = bounded(1);
        self.channels.get_block.0.send(GetBlock {
            hash: *hash,
            sender: tx,
        })?;
        let (id, done) = self.stopper.subscribe()?;
        select! {
            recv(done) -> _ => Err(Error::Stopped),
            recv(rx) -> result => {
                self.stopper.unsubscribe(id)?;
                result?
            }
        }
    }

    fn get_blockchain_height(&self) -> Result<u32, Error> {
        let (tx, rx) = bounded(1);
        self.channels
            .get_blockchain_height
            .0
            .send(GetBlockchainHeight { sender: tx })?;
        let (id, done) = self.stopper.subscribe()?;
        select! {
            recv(done) -> _ => Err(Error::Stopped),
            recv(rx) -> result => {
                self.stopper.unsubscribe(id)?;
                result?
            }
        }
    }
}

#[derive(Clone)]
struct BitcoinClientInner {
    client: Arc<Client>,
}

impl BitcoinClientInner {
    fn new(config: &Config) -> Result<Self, Error> {
        let client = Client::new(
            &config.rpc_address,
            Auth::UserPass(config.rpc_user.clone(), config.rpc_password.clone()),
        )?;

        Ok(BitcoinClientInner {
            client: Arc::new(client),
        })
    }
}

impl BitcoinRpc<Block> for BitcoinClientInner {
    fn get_block_hash(&self, height: u32) -> Result<BlockHash, Error> {
        Ok(self.client.get_block_hash(height as u64)?)
    }

    fn get_block(&self, hash: &BlockHash) -> Result<Box<Block>, Error> {
        Ok(Box::new(self.client.get_block(hash)?))
    }

    fn get_blockchain_height(&self) -> Result<u32, Error> {
        Ok(self.client.get_blockchain_info()?.blocks as u32)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bitcoincore_rpc::bitcoin::{
        absolute::LockTime,
        block::{self, Header},
        transaction, Amount, CompactTarget, OutPoint, ScriptBuf, ScriptHash, Sequence, Transaction,
        TxIn, TxMerkleNode, TxOut, Txid, Witness,
    };

    use crate::indexer::{
        test_utils::{test_block_hash, test_h160_hash, test_sha256_hash},
        types::entry::FromEntry,
    };

    use super::*;

    #[test]
    fn test_get_entries() {
        let height = 2;
        let script_pubkey = ScriptBuf::new_p2sh(&ScriptHash::from_byte_array(test_h160_hash(0)));
        let tx_in = TxIn {
            previous_output: OutPoint::new(Txid::from_slice(&test_sha256_hash(0)).unwrap(), 1),
            script_sig: ScriptBuf::from_bytes(test_h160_hash(0).to_vec()),
            sequence: Sequence(0xFFFFFFFF),
            witness: Witness::new(),
        };
        let tx_out = TxOut {
            value: Amount::MIN,
            script_pubkey: script_pubkey.clone(),
        };
        let tx = Transaction {
            version: transaction::Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![tx_in],
            output: vec![tx_out],
        };

        let block = Block {
            header: Header {
                version: block::Version::ONE,
                prev_blockhash: test_block_hash(1),
                merkle_root: TxMerkleNode::from_slice(&test_sha256_hash(height)).unwrap(),
                time: 1234567890,
                bits: CompactTarget::default(),
                nonce: 0,
            },
            txdata: vec![tx],
        };

        let entries = block.get_entries(Mode::Indexer, height);

        let entry = entries.first().unwrap().to_entry();
        let e = BlockAtHeightHasHash::from_entry(entry).unwrap();
        assert_eq!(e.height, height);
        assert_eq!(e.hash, block.block_hash().as_byte_array().to_owned());

        let entry = entries.get(1).unwrap().to_entry();
        let e = TxInBlockAtHeight::from_entry(entry).unwrap();
        assert_eq!(e.txid, block.txdata[0].compute_txid().to_byte_array());
        assert_eq!(e.height, height);

        let entry = entries.get(2).unwrap().to_entry();
        let e = BlockAtHeightSpentOutputInTx::from_entry(entry).unwrap();
        assert_eq!(e.txid, test_sha256_hash(0));
        assert_eq!(e.vout, 1);
        assert_eq!(e.height, height);

        let entry = entries.get(3).unwrap().to_entry();
        let e = ScriptHashHasOutputsInBlockAtHeight::from_entry(entry).unwrap();
        assert_eq!(e.script_hash, script_pubkey.script_hash().to_byte_array());
        assert_eq!(e.height, height);
    }
}
