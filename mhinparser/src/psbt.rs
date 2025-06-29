use bitcoin::psbt::Psbt as BitcoinPsbt;
use hex;

/// Wrapper around Bitcoin PSBT for easier handling
pub struct Psbt {
    inner: BitcoinPsbt,
}

impl Psbt {
    /// Convert PSBT to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.inner.serialize())
    }
}

impl From<BitcoinPsbt> for Psbt {
    fn from(psbt: BitcoinPsbt) -> Self {
        Self { inner: psbt }
    }
}