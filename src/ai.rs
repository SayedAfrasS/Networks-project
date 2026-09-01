use std::time::Duration;

use crate::congestion::CongestionController;
use crate::features::{FeatureExtractor, Features};

fn clamp01(value: f64) -> f64 {
    let value = if value.is_finite() { value } else { 0.0 };

    if value < 0.0 {
        0.0
    } else if value > 1.0 {
        1.0
    } else {
        value
    }
}

fn sigmoid(value: f64) -> f64 {
    let value = if value.is_finite() { value } else { 0.0 };

    let value = value.clamp(-20.0, 20.0);

    1.0 / (1.0 + (-value).exp())
}

pub struct SimpleAiPredictor {
    weights: [f64; 4],
    bias: f64,
    learning_rate: f64,
}

impl SimpleAiPredictor {
    pub fn new() -> Self {
        Self {
            weights: [0.35, 0.25, 0.20, 0.20],
            bias: 0.0,
            learning_rate: 0.05,
        }
    }

    fn feature_vector(features: &Features) -> [f64; 4] {
        let avg_for_risk = features.avg_rtt_us.max(1000) as f64;

        let trend_norm = if features.rtt_trend_us > 0 {
            clamp01(features.rtt_trend_us as f64 / avg_for_risk)
        } else {
            0.0
        };

        let inflation_norm =
            if features.min_rtt_us > 0 && features.latest_rtt_us > features.min_rtt_us {
                clamp01(
                    (features.latest_rtt_us as f64 / features.min_rtt_us as f64) - 1.0,
                )
            } else {
                0.0
            };

        let jitter_norm = clamp01(features.jitter_us as f64 / avg_for_risk);

        let loss_norm = clamp01(features.loss_rate * 2.0);

        [trend_norm, inflation_norm, jitter_norm, loss_norm]
    }

    pub fn predict(&self, features: &Features) -> f64 {
        let x = Self::feature_vector(features);

        let mut z = self.bias;

        for i in 0..4 {
            z += self.weights[i] * x[i];
        }

        sigmoid(z)
    }

    pub fn train(&mut self, features: &Features, label: f64) {
        let label = clamp01(label);

        let prediction = self.predict(features);

        let error = label - prediction;

        let x = Self::feature_vector(features);

        for i in 0..4 {
            let update = self.learning_rate * error * x[i];

            self.weights[i] = (self.weights[i] + update).clamp(-5.0, 5.0);
        }

        self.bias = (self.bias + self.learning_rate * error).clamp(-5.0, 5.0);
    }

    pub fn status(&self) -> String {
        format!(
            "w=[{:.2},{:.2},{:.2},{:.2}] b={:.2}",
            self.weights[0],
            self.weights[1],
            self.weights[2],
            self.weights[3],
            self.bias
        )
    }
}

pub struct AiCongestionController {
    features: FeatureExtractor,
    predictor: SimpleAiPredictor,

    cwnd_bytes: usize,
    in_flight_bytes: usize,

    mss: usize,
    min_cwnd: usize,
    max_cwnd: usize,

    ack_events: u64,
    acks_since_reduction: u64,
    proactive_reductions: u64,
    loss_events: u64,
    retransmit_events: u64,

    pending_retransmits: u64,

    last_ai_risk: f64,
    last_heuristic_risk: f64,
    last_combined_risk: f64,
}

impl AiCongestionController {
    pub fn new(mss: usize) -> Self {
        let mss = if mss == 0 { 1200 } else { mss };

        Self {
            features: FeatureExtractor::new(16),
            predictor: SimpleAiPredictor::new(),

            cwnd_bytes: mss.saturating_mul(4),
            in_flight_bytes: 0,

            mss,
            min_cwnd: mss,
            max_cwnd: mss.saturating_mul(64),

            ack_events: 0,
            acks_since_reduction: 0,
            proactive_reductions: 0,
            loss_events: 0,
            retransmit_events: 0,

            pending_retransmits: 0,

            last_ai_risk: 0.0,
            last_heuristic_risk: 0.0,
            last_combined_risk: 0.0,
        }
    }
}

impl CongestionController for AiCongestionController {
    fn name(&self) -> &str {
        "simple-ai"
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

        let label = if self.pending_retransmits > 0 {
            1.0
        } else {
            0.0
        };

        self.predictor.train(&features, label);

        self.pending_retransmits = 0;

        let ai_risk = self.predictor.predict(&features);

        let heuristic_risk = features.risk;

        let combined_risk = clamp01(0.6 * ai_risk + 0.4 * heuristic_risk);

        self.last_ai_risk = ai_risk;
        self.last_heuristic_risk = heuristic_risk;
        self.last_combined_risk = combined_risk;

        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        if combined_risk >= 0.65 && self.acks_since_reduction >= 2 {
            self.cwnd_bytes = self
                .cwnd_bytes
                .saturating_mul(75)
                .checked_div(100)
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

        if combined_risk < 0.35 {
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
        } else if combined_risk < 0.65 && self.ack_events % 8 == 0 {
            self.cwnd_bytes = self
                .cwnd_bytes
                .saturating_add(1)
                .min(self.max_cwnd);
        }
    }

    fn on_loss(&mut self, packet_size: usize) {
        self.loss_events = self.loss_events.saturating_add(1);

        self.features.record_loss();

        let features = self.features.features();

        self.predictor.train(&features, 1.0);

        let ai_risk = self.predictor.predict(&features);

        let heuristic_risk = features.risk;

        let combined_risk = clamp01(0.6 * ai_risk + 0.4 * heuristic_risk);

        self.last_ai_risk = ai_risk;
        self.last_heuristic_risk = heuristic_risk;
        self.last_combined_risk = combined_risk;

        self.pending_retransmits = 0;

        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        self.cwnd_bytes = (self.cwnd_bytes / 2).max(self.min_cwnd);

        self.acks_since_reduction = 0;
    }

    fn on_retransmit(&mut self) {
        self.retransmit_events = self.retransmit_events.saturating_add(1);
        self.pending_retransmits = self.pending_retransmits.saturating_add(1);
    }

    fn status(&self) -> String {
        let features = self.features.features();

        format!(
            "ai cwnd_bytes={} in_flight_bytes={} mss={} ai_risk={:.2} combined_risk={:.2} heuristic_risk={:.2} loss_rate={:.2} jitter_us={} trend_us={} proactive_reductions={} loss_events={} retx_events={}",
            self.cwnd_bytes,
            self.in_flight_bytes,
            self.mss,
            self.last_ai_risk,
            self.last_combined_risk,
            self.last_heuristic_risk,
            features.loss_rate,
            features.jitter_us,
            features.rtt_trend_us,
            self.proactive_reductions,
            self.loss_events,
            self.retransmit_events
        )
    }

    fn features_text(&self) -> String {
        let features = self.features.features();

        format!(
            "samples={} latest_rtt_us={} avg_rtt_us={} min_rtt_us={} max_rtt_us={} trend_us={} jitter_us={} loss_rate={:.2} heuristic_risk={:.2} ai_risk={:.2} combined_risk={:.2} predictor={}",
            features.samples,
            features.latest_rtt_us,
            features.avg_rtt_us,
            features.min_rtt_us,
            features.max_rtt_us,
            features.rtt_trend_us,
            features.jitter_us,
            features.loss_rate,
            features.risk,
            self.last_ai_risk,
            self.last_combined_risk,
            self.predictor.status()
        )
    }

    fn cwnd_bytes(&self) -> usize {
        self.cwnd_bytes
    }

    fn risk(&self) -> f64 {
        self.last_combined_risk
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predictor_returns_valid_risk() {
        let mut predictor = SimpleAiPredictor::new();

        let features = Features {
            samples: 4,
            latest_rtt_us: 2000,
            avg_rtt_us: 1500,
            min_rtt_us: 1000,
            max_rtt_us: 2000,
            rtt_trend_us: 200,
            jitter_us: 300,
            loss_rate: 0.1,
            risk: 0.3,
        };

        let before = predictor.predict(&features);

        predictor.train(&features, 1.0);

        let after = predictor.predict(&features);

        assert!(before >= 0.0);
        assert!(before <= 1.0);

        assert!(after >= 0.0);
        assert!(after <= 1.0);
    }
}