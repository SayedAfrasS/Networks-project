use std::time::{SystemTime, UNIX_EPOCH};

pub const VERSION: u8 = 1;

pub const TYPE_DATA: u8 = 0;
pub const TYPE_ACK: u8 = 1;

pub const HEADER_LEN: usize = 28;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reliability {
    BestEffort,
    Important,
    Guaranteed,
}

impl Reliability {
    pub fn to_u8(self) -> u8 {
        match self {
            Reliability::BestEffort => 0,
            Reliability::Important => 1,
            Reliability::Guaranteed => 2,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Reliability::BestEffort),
            1 => Some(Reliability::Important),
            2 => Some(Reliability::Guaranteed),
            _ => None,
        }
    }
}

pub struct Packet {
    pub version: u8,
    pub ptype: u8,
    pub reliability: Reliability,
    pub priority: u8,
    pub connection_id: u32,
    pub stream_id: u32,
    pub seq: u32,
    pub ack: u32,
    pub timestamp_us: u64,
    pub payload: Vec<u8>,
}

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub fn encode_packet(
    ptype: u8,
    reliability: Reliability,
    priority: u8,
    connection_id: u32,
    stream_id: u32,
    seq: u32,
    ack: u32,
    payload: &[u8],
) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(HEADER_LEN + payload.len());

    buffer.push(VERSION);
    buffer.push(ptype);
    buffer.push(reliability.to_u8());
    buffer.push(priority);

    buffer.extend_from_slice(&connection_id.to_be_bytes());
    buffer.extend_from_slice(&stream_id.to_be_bytes());
    buffer.extend_from_slice(&seq.to_be_bytes());
    buffer.extend_from_slice(&ack.to_be_bytes());
    buffer.extend_from_slice(&now_us().to_be_bytes());

    buffer.extend_from_slice(payload);

    buffer
}

pub fn decode_packet(buffer: &[u8]) -> Result<Packet, String> {
    if buffer.len() < HEADER_LEN {
        return Err("packet too short".to_string());
    }

    let version = buffer[0];
    let ptype = buffer[1];

    let reliability = Reliability::from_u8(buffer[2])
        .ok_or_else(|| "invalid reliability value".to_string())?;

    let priority = buffer[3];

    let connection_id = u32::from_be_bytes([
        buffer[4],
        buffer[5],
        buffer[6],
        buffer[7],
    ]);

    let stream_id = u32::from_be_bytes([
        buffer[8],
        buffer[9],
        buffer[10],
        buffer[11],
    ]);

    let seq = u32::from_be_bytes([
        buffer[12],
        buffer[13],
        buffer[14],
        buffer[15],
    ]);

    let ack = u32::from_be_bytes([
        buffer[16],
        buffer[17],
        buffer[18],
        buffer[19],
    ]);

    let mut timestamp_bytes = [0u8; 8];
    timestamp_bytes.copy_from_slice(&buffer[20..28]);
    let timestamp_us = u64::from_be_bytes(timestamp_bytes);

    let payload = buffer[HEADER_LEN..].to_vec();

    Ok(Packet {
        version,
        ptype,
        reliability,
        priority,
        connection_id,
        stream_id,
        seq,
        ack,
        timestamp_us,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_roundtrip_works() {
        let payload = b"hello";

        let encoded = encode_packet(
            TYPE_DATA,
            Reliability::Important,
            7,
            11,
            22,
            33,
            0,
            payload,
        );

        let decoded = decode_packet(&encoded).expect("packet should decode");

        assert_eq!(decoded.version, VERSION);
        assert_eq!(decoded.ptype, TYPE_DATA);
        assert_eq!(decoded.reliability, Reliability::Important);
        assert_eq!(decoded.priority, 7);
        assert_eq!(decoded.connection_id, 11);
        assert_eq!(decoded.stream_id, 22);
        assert_eq!(decoded.seq, 33);
        assert_eq!(decoded.ack, 0);
        assert_eq!(decoded.payload, payload.to_vec());
    }
}