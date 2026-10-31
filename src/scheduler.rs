use std::collections::{HashMap, VecDeque};
use crate::packet::Reliability;

#[derive(Debug, Clone)]
pub struct ScheduledPacket {
    pub priority: u8,
    pub stream_id: u32,
    pub seq: u32,
    pub reliability: Reliability,
    pub encoded: Vec<u8>,
    pub payload_preview: String,
    pub created_us: u64,
    pub deadline_us: u64,
}

impl ScheduledPacket {
    pub fn new(
        priority: u8,
        seq: u32,
        reliability: Reliability,
        encoded: Vec<u8>,
        payload_preview: String,
    ) -> Self {
        Self {
            priority,
            stream_id: 1,
            seq,
            reliability,
            encoded,
            payload_preview,
            created_us: 0,
            deadline_us: 0,
        }
    }

    pub fn with_stream(mut self, stream_id: u32) -> Self {
        self.stream_id = stream_id;
        self
    }

    pub fn with_deadline(mut self, created_us: u64, deadline_us: u64) -> Self {
        self.created_us = created_us;
        self.deadline_us = deadline_us;
        self
    }
}

pub struct PriorityScheduler {
    queue: Vec<ScheduledPacket>,
}

impl PriorityScheduler {
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    pub fn enqueue(&mut self, packet: ScheduledPacket) {
        self.queue.push(packet);
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn pop_next(&mut self) -> Option<ScheduledPacket> {
        if self.queue.is_empty() {
            return None;
        }

        let mut best_index = 0;

        for i in 1..self.queue.len() {
            let best = &self.queue[best_index];
            let current = &self.queue[i];

            if current.priority > best.priority
                || (current.priority == best.priority && current.seq < best.seq)
            {
                best_index = i;
            }
        }

        Some(self.queue.remove(best_index))
    }
}

pub struct StreamQueue {
    pub stream_id: u32,
    pub priority: u8,
    pub quantum_bytes: usize,
    pub deficit_bytes: usize,
    pub queue: VecDeque<ScheduledPacket>,
}

pub struct DeficitRoundRobinScheduler {
    streams: HashMap<u32, StreamQueue>,
    active_stream_ids: VecDeque<u32>,
    base_quantum: usize,
    total_packets: usize,
}

impl DeficitRoundRobinScheduler {
    pub fn new(base_quantum: usize) -> Self {
        let base_quantum = if base_quantum == 0 { 1500 } else { base_quantum };
        Self {
            streams: HashMap::new(),
            active_stream_ids: VecDeque::new(),
            base_quantum,
            total_packets: 0,
        }
    }

    pub fn register_stream(&mut self, stream_id: u32, priority: u8) {
        let weight = (priority as usize).max(1);
        let quantum_bytes = self.base_quantum.saturating_mul(weight);

        if let Some(existing) = self.streams.get_mut(&stream_id) {
            existing.priority = priority;
            existing.quantum_bytes = quantum_bytes;
        } else {
            self.streams.insert(
                stream_id,
                StreamQueue {
                    stream_id,
                    priority,
                    quantum_bytes,
                    deficit_bytes: 0,
                    queue: VecDeque::new(),
                },
            );
        }
    }

    pub fn enqueue(&mut self, packet: ScheduledPacket) {
        let stream_id = packet.stream_id;
        let priority = packet.priority;

        if !self.streams.contains_key(&stream_id) {
            self.register_stream(stream_id, priority);
        }

        if let Some(sq) = self.streams.get_mut(&stream_id) {
            let was_empty = sq.queue.is_empty();
            sq.queue.push_back(packet);
            self.total_packets += 1;

            if was_empty && !self.active_stream_ids.contains(&stream_id) {
                self.active_stream_ids.push_back(stream_id);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.total_packets
    }

    pub fn is_empty(&self) -> bool {
        self.total_packets == 0
    }

    pub fn pop_next(&mut self) -> Option<ScheduledPacket> {
        if self.total_packets == 0 || self.active_stream_ids.is_empty() {
            return None;
        }

        let num_active = self.active_stream_ids.len();
        for _ in 0..num_active {
            let stream_id = match self.active_stream_ids.pop_front() {
                Some(id) => id,
                None => break,
            };

            let (should_requeue, packet) = if let Some(sq) = self.streams.get_mut(&stream_id) {
                if sq.queue.is_empty() {
                    sq.deficit_bytes = 0;
                    (false, None)
                } else {
                    sq.deficit_bytes = sq.deficit_bytes.saturating_add(sq.quantum_bytes);

                    let next_size = sq.queue.front().map(|p| p.encoded.len()).unwrap_or(0);
                    if next_size <= sq.deficit_bytes {
                        let pkt = sq.queue.pop_front();
                        sq.deficit_bytes = sq.deficit_bytes.saturating_sub(next_size);

                        let keep_active = !sq.queue.is_empty();
                        if !keep_active {
                            sq.deficit_bytes = 0;
                        }
                        (keep_active, pkt)
                    } else {
                        (true, None)
                    }
                }
            } else {
                (false, None)
            };

            if should_requeue {
                self.active_stream_ids.push_back(stream_id);
            }

            if let Some(pkt) = packet {
                self.total_packets = self.total_packets.saturating_sub(1);
                return Some(pkt);
            }
        }

        None
    }
}

pub enum PacketScheduler {
    Priority(PriorityScheduler),
    Drr(DeficitRoundRobinScheduler),
}

impl PacketScheduler {
    pub fn new_priority() -> Self {
        PacketScheduler::Priority(PriorityScheduler::new())
    }

    pub fn new_drr(base_quantum: usize) -> Self {
        PacketScheduler::Drr(DeficitRoundRobinScheduler::new(base_quantum))
    }

    pub fn enqueue(&mut self, packet: ScheduledPacket) {
        match self {
            PacketScheduler::Priority(p) => p.enqueue(packet),
            PacketScheduler::Drr(d) => d.enqueue(packet),
        }
    }

    pub fn pop_next(&mut self) -> Option<ScheduledPacket> {
        match self {
            PacketScheduler::Priority(p) => p.pop_next(),
            PacketScheduler::Drr(d) => d.pop_next(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            PacketScheduler::Priority(p) => p.len(),
            PacketScheduler::Drr(d) => d.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            PacketScheduler::Priority(p) => p.is_empty(),
            PacketScheduler::Drr(d) => d.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_packet(priority: u8, seq: u32, stream_id: u32) -> ScheduledPacket {
        ScheduledPacket {
            priority,
            stream_id,
            seq,
            reliability: Reliability::BestEffort,
            encoded: vec![0; 100],
            payload_preview: format!("packet-{}", seq),
            created_us: 0,
            deadline_us: 0,
        }
    }

    #[test]
    fn scheduler_prefers_higher_priority_first() {
        let mut scheduler = PriorityScheduler::new();

        scheduler.enqueue(make_packet(1, 1, 1));
        scheduler.enqueue(make_packet(7, 2, 1));
        scheduler.enqueue(make_packet(3, 3, 1));

        let first = scheduler.pop_next().expect("should have packet");
        assert_eq!(first.seq, 2);
        assert_eq!(first.priority, 7);

        let second = scheduler.pop_next().expect("should have packet");
        assert_eq!(second.seq, 3);
        assert_eq!(second.priority, 3);

        let third = scheduler.pop_next().expect("should have packet");
        assert_eq!(third.seq, 1);
        assert_eq!(third.priority, 1);
    }

    #[test]
    fn drr_serves_streams_fairly() {
        let mut drr = DeficitRoundRobinScheduler::new(200);

        drr.register_stream(1, 1); // quantum = 200 (can send 2 pkts of 100 bytes)
        drr.register_stream(2, 1); // quantum = 200

        drr.enqueue(make_packet(1, 1, 1));
        drr.enqueue(make_packet(1, 2, 1));
        drr.enqueue(make_packet(1, 3, 2));
        drr.enqueue(make_packet(1, 4, 2));

        let p1 = drr.pop_next().unwrap();
        let p2 = drr.pop_next().unwrap();
        let p3 = drr.pop_next().unwrap();
        let p4 = drr.pop_next().unwrap();

        assert_eq!(p1.stream_id, 1);
        assert_eq!(p2.stream_id, 1);
        assert_eq!(p3.stream_id, 2);
        assert_eq!(p4.stream_id, 2);
    }
}
