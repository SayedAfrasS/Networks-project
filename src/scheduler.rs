use crate::packet::Reliability;

#[derive(Debug, Clone)]
pub struct ScheduledPacket {
    pub priority: u8,
    pub seq: u32,
    pub reliability: Reliability,
    pub encoded: Vec<u8>,
    pub payload_preview: String,
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

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Higher priority value is sent first.
    /// If priority is equal, lower sequence number is sent first.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_packet(priority: u8, seq: u32) -> ScheduledPacket {
        ScheduledPacket {
            priority,
            seq,
            reliability: Reliability::BestEffort,
            encoded: vec![0],
            payload_preview: format!("packet-{}", seq),
        }
    }

    #[test]
    fn scheduler_prefers_higher_priority_first() {
        let mut scheduler = PriorityScheduler::new();

        scheduler.enqueue(make_packet(1, 1));
        scheduler.enqueue(make_packet(7, 2));
        scheduler.enqueue(make_packet(3, 3));

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
}