use std::fs::File;
use std::io::{self};
use std::io::{Read, Write};

pub fn inverse_hash(hashstring: &str) -> String {
    hashstring
        .chars()
        .rev()
        .collect::<Vec<char>>()
        .chunks(2)
        .flat_map(|chunk| chunk.iter().rev())
        .collect::<String>()
}

/// Read a varint from a file
pub fn read_varint(file: &mut File) -> io::Result<u64> {
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;

    let first_byte = buf[0];
    if first_byte < 0xFD {
        return Ok(first_byte as u64);
    } else if first_byte == 0xFD {
        let mut buf = [0u8; 2];
        file.read_exact(&mut buf)?;
        return Ok(u16::from_le_bytes(buf) as u64);
    } else if first_byte == 0xFE {
        let mut buf = [0u8; 4];
        file.read_exact(&mut buf)?;
        return Ok(u32::from_le_bytes(buf) as u64);
    } else {
        // 0xFF
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        return Ok(u64::from_le_bytes(buf));
    }
}

/// Calculate the size of a varint
pub fn calc_varint_size(value: u64) -> u64 {
    if value < 0xFD {
        1
    } else if value <= 0xFFFF {
        3
    } else if value <= 0xFFFFFFFF {
        5
    } else {
        9
    }
}

/// Write a varint to a file
pub fn write_varint(file: &mut File, value: u64) -> io::Result<()> {
    if value < 0xFD {
        file.write_all(&[value as u8])?;
    } else if value <= 0xFFFF {
        file.write_all(&[0xFD])?;
        file.write_all(&(value as u16).to_le_bytes())?;
    } else if value <= 0xFFFFFFFF {
        file.write_all(&[0xFE])?;
        file.write_all(&(value as u32).to_le_bytes())?;
    } else {
        file.write_all(&[0xFF])?;
        file.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}
