use std::time::Duration;

pub trait CongestionController {
    fn name(&self) -> &str;

    fn can_send(&self, packet_size: usize) -> bool;

    fn on_packet_sent(&mut self, packet_size: usize);

    fn on_ack(&mut self, packet_size: usize, rtt: Duration);

    fn on_loss(&mut self, packet_size: usize);

    fn status(&self) -> String;
}

pub struct SimpleAimd {
    cwnd_bytes: usize,
    in_flight_bytes: usize,
    mss: usize,
    min_cwnd: usize,
    max_cwnd: usize,
}

impl SimpleAimd {
    pub fn new(mss: usize) -> Self {
        let mss = if mss == 0 { 1200 } else { mss };

        Self {
            cwnd_bytes: mss.saturating_mul(4),
            in_flight_bytes: 0,
            mss,
            min_cwnd: mss,
            max_cwnd: mss.saturating_mul(64),
        }
    }
}

impl CongestionController for SimpleAimd {
    fn name(&self) -> &str {
        "simple-aimd"
    }

    fn can_send(&self, packet_size: usize) -> bool {
        // If nothing is in flight, allow at least one packet.
        // This avoids blocking forever when packet_size > cwnd_bytes.
        if self.in_flight_bytes == 0 {
            return true;
        }

        self.in_flight_bytes
            .saturating_add(packet_size)
            <= self.cwnd_bytes
    }

    fn on_packet_sent(&mut self, packet_size: usize) {
        self.in_flight_bytes = self.in_flight_bytes.saturating_add(packet_size);
    }

    fn on_ack(&mut self, packet_size: usize, _rtt: Duration) {
        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        if self.cwnd_bytes == 0 {
            self.cwnd_bytes = self.min_cwnd;
        }

        // Simple additive-increase approximation.
        // This is not a final congestion control algorithm.
        // It is only a modular baseline.
        let increase = self
            .mss
            .saturating_mul(packet_size)
            .checked_div(self.cwnd_bytes)
            .unwrap_or(1);

        let increase = if increase == 0 { 1 } else { increase };

        self.cwnd_bytes = self
            .cwnd_bytes
            .saturating_add(increase)
            .min(self.max_cwnd);
    }

    fn on_loss(&mut self, packet_size: usize) {
        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        // Multiplicative decrease.
        self.cwnd_bytes = (self.cwnd_bytes / 2).max(self.min_cwnd);
    }

    fn status(&self) -> String {
        format!(
            "cwnd_bytes={} in_flight_bytes={} mss={}",
            self.cwnd_bytes, self.in_flight_bytes, self.mss
        )
    }
}