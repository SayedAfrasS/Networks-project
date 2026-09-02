use std::collections::HashMap;
use std::time::Duration;

use crate::packet::Reliability;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Closing,
}

#[derive(Debug, Default, Clone)]
pub struct StreamStats {
    pub sent: u64,
    pub acked: u64,
    pub lost: u64,
    pub retransmits: u64,
    pub in_flight: u64,
    pub rtt_samples: u64,
    pub rtt_sum_us: u128,
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub id: u32,
    pub reliability: Reliability,
    pub priority: u8,
    pub stats: StreamStats,
}

pub struct ConnectionManager {
    pub connection_id: u32,
    pub state: ConnectionState,
    pub current_stream: u32,
    pub streams: HashMap<u32, StreamInfo>,
    next_stream_id: u32,
    max_stream_in_flight: u64,
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
                stats: StreamStats::default(),
            },
        );

        Self {
            connection_id,
            state: ConnectionState::Disconnected,
            current_stream: 1,
            streams,
            next_stream_id: 2,
            max_stream_in_flight: 4,
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
                stats: StreamStats::default(),
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

    pub fn can_send_stream(&self, stream_id: u32, reliability: Reliability) -> bool {
        if reliability == Reliability::BestEffort {
            return true;
        }

        match self.streams.get(&stream_id) {
            Some(stream) => stream.stats.in_flight < self.max_stream_in_flight,
            None => true,
        }
    }

    pub fn record_send(&mut self, stream_id: u32, reliability: Reliability) {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.stats.sent = stream.stats.sent.saturating_add(1);

            if reliability != Reliability::BestEffort {
                stream.stats.in_flight = stream.stats.in_flight.saturating_add(1);
            }
        }
    }

    pub fn record_ack(&mut self, stream_id: u32, rtt: Duration) {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.stats.acked = stream.stats.acked.saturating_add(1);

            stream.stats.in_flight = stream.stats.in_flight.saturating_sub(1);

            stream.stats.rtt_samples = stream.stats.rtt_samples.saturating_add(1);

            stream.stats.rtt_sum_us = stream
                .stats
                .rtt_sum_us
                .saturating_add(rtt.as_micros());
        }
    }

    pub fn record_loss(&mut self, stream_id: u32) {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.stats.lost = stream.stats.lost.saturating_add(1);

            stream.stats.in_flight = stream.stats.in_flight.saturating_sub(1);
        }
    }

    pub fn record_retransmit(&mut self, stream_id: u32) {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.stats.retransmits = stream.stats.retransmits.saturating_add(1);
        }
    }

    pub fn stream_stats_text(&self) -> String {
        let mut lines = Vec::new();

        for stream_id in self.stream_ids() {
            if let Some(stream) = self.streams.get(&stream_id) {
                let avg_rtt_us = if stream.stats.rtt_samples > 0 {
                    stream.stats.rtt_sum_us / stream.stats.rtt_samples as u128
                } else {
                    0
                };

                lines.push(format!(
                    "stream={} rel={:?} prio={} sent={} acked={} lost={} retx={} inflight={} avg_rtt_us={}",
                    stream.id,
                    stream.reliability,
                    stream.priority,
                    stream.stats.sent,
                    stream.stats.acked,
                    stream.stats.lost,
                    stream.stats.retransmits,
                    stream.stats.in_flight,
                    avg_rtt_us
                ));
            }
        }

        lines.join("\n")
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

    #[test]
    fn connection_records_stream_stats() {
        let mut connection = ConnectionManager::new(123);

        let id = connection.create_stream(Reliability::Guaranteed, 3);

        connection.record_send(id, Reliability::Guaranteed);
        connection.record_ack(id, Duration::from_micros(500));

        let stream = connection.streams.get(&id).expect("stream should exist");

        assert_eq!(stream.stats.sent, 1);
        assert_eq!(stream.stats.acked, 1);
        assert_eq!(stream.stats.in_flight, 0);
        assert_eq!(stream.stats.rtt_samples, 1);
    }
}