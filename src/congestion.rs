use std::time::Duration;

use crate::features::FeatureExtractor;

pub trait CongestionController {
    fn name(&self) -> &str;

    fn can_send(&self, packet_size: usize) -> bool;

    fn on_packet_sent(&mut self, packet_size: usize);

    fn on_ack(&mut self, packet_size: usize, rtt: Duration);

    fn on_loss(&mut self, packet_size: usize);

    fn status(&self) -> String;

    fn features_text(&self) -> String {
        String::from("no feature extraction")
    }

    fn cwnd_bytes(&self) -> usize {
        0
    }

    fn risk(&self) -> f64 {
        0.0
    }
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

        self.cwnd_bytes = (self.cwnd_bytes / 2).max(self.min_cwnd);
    }

    fn status(&self) -> String {
        format!(
            "simple-aimd cwnd_bytes={} in_flight_bytes={} mss={}",
            self.cwnd_bytes, self.in_flight_bytes, self.mss
        )
    }

        fn cwnd_bytes(&self) -> usize {
        self.cwnd_bytes
    }

    fn risk(&self) -> f64 {
        0.0
    }
}

pub struct PredictiveController {
    features: FeatureExtractor,

    cwnd_bytes: usize,
    in_flight_bytes: usize,

    mss: usize,
    min_cwnd: usize,
    max_cwnd: usize,

    ack_events: u64,
    acks_since_reduction: u64,
    proactive_reductions: u64,
    loss_events: u64,
}

impl PredictiveController {
    pub fn new(mss: usize) -> Self {
        let mss = if mss == 0 { 1200 } else { mss };

        Self {
            features: FeatureExtractor::new(16),

            cwnd_bytes: mss.saturating_mul(4),
            in_flight_bytes: 0,

            mss,
            min_cwnd: mss,
            max_cwnd: mss.saturating_mul(64),

            ack_events: 0,
            acks_since_reduction: 0,
            proactive_reductions: 0,
            loss_events: 0,
        }
    }
}

impl CongestionController for PredictiveController {
    fn name(&self) -> &str {
        "predictive-risk"
    }

    fn can_send(&self, packet_size: usize) -> bool {
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

    fn on_ack(&mut self, packet_size: usize, rtt: Duration) {
        self.ack_events = self.ack_events.saturating_add(1);
        self.acks_since_reduction = self.acks_since_reduction.saturating_add(1);

        self.features.record_ack(rtt);

        let features = self.features.features();

        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        // Proactive reduction if congestion risk is high.
        if features.risk >= 0.65 && self.acks_since_reduction >= 3 {
            self.cwnd_bytes = self
                .cwnd_bytes
                .saturating_mul(8)
                .checked_div(10)
                .unwrap_or(self.min_cwnd);

            if self.cwnd_bytes < self.min_cwnd {
                self.cwnd_bytes = self.min_cwnd;
            }

            self.acks_since_reduction = 0;
            self.proactive_reductions = self.proactive_reductions.saturating_add(1);

            return;
        }

        if self.cwnd_bytes == 0 {
            self.cwnd_bytes = self.min_cwnd;
        }

        // Increase only when risk is low.
        if features.risk < 0.35 {
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
        } else if features.risk < 0.65 && self.ack_events % 8 == 0 {
            // Very slow increase under medium risk.
            self.cwnd_bytes = self
                .cwnd_bytes
                .saturating_add(1)
                .min(self.max_cwnd);
        }
    }

    fn on_loss(&mut self, packet_size: usize) {
        self.loss_events = self.loss_events.saturating_add(1);

        self.features.record_loss();

        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        self.cwnd_bytes = (self.cwnd_bytes / 2).max(self.min_cwnd);

        self.acks_since_reduction = 0;
    }

    fn status(&self) -> String {
        let features = self.features.features();

        format!(
            "predictive cwnd_bytes={} in_flight_bytes={} mss={} risk={:.2} loss_rate={:.2} jitter_us={} trend_us={} proactive_reductions={} loss_events={}",
            self.cwnd_bytes,
            self.in_flight_bytes,
            self.mss,
            features.risk,
            features.loss_rate,
            features.jitter_us,
            features.rtt_trend_us,
            self.proactive_reductions,
            self.loss_events
        )
    }

    fn features_text(&self) -> String {
        let features = self.features.features();

        format!(
            "samples={} latest_rtt_us={} avg_rtt_us={} min_rtt_us={} max_rtt_us={} trend_us={} jitter_us={} loss_rate={:.2} risk={:.2}",
            features.samples,
            features.latest_rtt_us,
            features.avg_rtt_us,
            features.min_rtt_us,
            features.max_rtt_us,
            features.rtt_trend_us,
            features.jitter_us,
            features.loss_rate,
            features.risk
        )
    }

    fn cwnd_bytes(&self) -> usize {
        self.cwnd_bytes
    }

    fn risk(&self) -> f64 {
        0.0
    }
}