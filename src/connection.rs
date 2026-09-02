use std::collections::HashMap;

use crate::packet::Reliability;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Closing,
}

#[derive(Debug, Clone, Copy)]
pub struct StreamInfo {
    pub id: u32,
    pub reliability: Reliability,
    pub priority: u8,
}

pub struct ConnectionManager {
    pub connection_id: u32,
    pub state: ConnectionState,
    pub current_stream: u32,
    pub streams: HashMap<u32, StreamInfo>,
    next_stream_id: u32,
}

impl ConnectionManager {
    pub fn new(connection_id: u32) -> Self {
        let mut streams = HashMap::new();

        streams.insert(
            1,
            StreamInfo {
                id: 1,
                reliability: Reliability::Guaranteed,
                priority: 0,
            },
        );

        Self {
            connection_id,
            state: ConnectionState::Disconnected,
            current_stream: 1,
            streams,
            next_stream_id: 2,
        }
    }

    pub fn create_stream(&mut self, reliability: Reliability, priority: u8) -> u32 {
        let id = self.next_stream_id;

        self.next_stream_id = self.next_stream_id.wrapping_add(1);

        self.streams.insert(
            id,
            StreamInfo {
                id,
                reliability,
                priority,
            },
        );

        self.current_stream = id;

        id
    }

    pub fn use_stream(&mut self, id: u32) -> bool {
        if self.streams.contains_key(&id) {
            self.current_stream = id;
            true
        } else {
            false
        }
    }

    pub fn stream_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.streams.keys().cloned().collect();

        ids.sort();

        ids
    }

    pub fn status(&self) -> String {
        format!(
            "conn_id={} state={:?} current_stream={} streams={}",
            self.connection_id,
            self.state,
            self.current_stream,
            self.stream_ids().len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_creates_and_uses_stream() {
        let mut connection = ConnectionManager::new(123);

        let id = connection.create_stream(Reliability::Important, 5);

        assert!(connection.use_stream(id));
        assert_eq!(connection.current_stream, id);
    }
}