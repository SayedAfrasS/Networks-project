use std::collections::VecDeque;
use std::time::Duration;

use crate::congestion::CongestionController;
use crate::features::{clamp01, FeatureExtractor, Features};
use crate::packet::now_us;

pub fn sigmoid(value: f64) -> f64 {
    let value = if value.is_finite() { value } else { 0.0 };
    let value = value.clamp(-20.0, 20.0);
    1.0 / (1.0 + (-value).exp())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PredictionMetrics {
    pub total_predictions: u64,
    pub true_positives: u64,
    pub false_positives: u64,
    pub true_negatives: u64,
    pub false_negatives: u64,
    pub bce_loss_sum: f64,
    pub lead_time_sum_us: u128,
    pub lead_time_samples: u64,
    pub min_lead_time_us: Option<u64>,
    pub max_lead_time_us: Option<u64>,
}

impl PredictionMetrics {
    pub fn precision(&self) -> f64 {
        let denom = self.true_positives + self.false_positives;
        if denom == 0 {
            1.0
        } else {
            self.true_positives as f64 / denom as f64
        }
    }

    pub fn recall(&self) -> f64 {
        let denom = self.true_positives + self.false_negatives;
        if denom == 0 {
            1.0
        } else {
            self.true_positives as f64 / denom as f64
        }
    }

    pub fn f1_score(&self) -> f64 {
        let p = self.precision();
        let r = self.recall();
        if p + r == 0.0 {
            0.0
        } else {
            2.0 * p * r / (p + r)
        }
    }

    pub fn avg_bce_loss(&self) -> f64 {
        if self.total_predictions == 0 {
            0.0
        } else {
            self.bce_loss_sum / self.total_predictions as f64
        }
    }

    pub fn avg_lead_time_us(&self) -> u64 {
        if self.lead_time_samples == 0 {
            0
        } else {
            (self.lead_time_sum_us / self.lead_time_samples as u128) as u64
        }
    }
}

#[derive(Debug, Clone)]
struct HorizonSample {
    timestamp_us: u64,
    features: [f64; 4],
    predicted_probability: f64,
    horizon_k: u32,
    packets_remaining: u32,
    high_risk_flagged: bool,
}

#[derive(Debug, Clone)]
pub struct OnlineLogisticPredictor {
    pub weights: [f64; 4],
    pub bias: f64,
    pub learning_rate: f64,
    pub horizon_k: u32,
    pub classification_threshold: f64,
    pub metrics: PredictionMetrics,
    horizon_queue: VecDeque<HorizonSample>,
}

impl OnlineLogisticPredictor {
    pub fn new() -> Self {
        Self {
            weights: [0.35, 0.25, 0.20, 0.20],
            bias: -0.20,
            learning_rate: 0.05,
            horizon_k: 5,
            classification_threshold: 0.50,
            metrics: PredictionMetrics::default(),
            horizon_queue: VecDeque::with_capacity(64),
        }
    }

    pub fn set_learning_rate(&mut self, lr: f64) {
        self.learning_rate = lr.clamp(0.0001, 1.0);
    }

    pub fn set_horizon(&mut self, k: u32) {
        self.horizon_k = k.clamp(1, 20);
    }

    pub fn predict_features(&self, f_arr: &[f64; 4]) -> f64 {
        let mut z = self.bias;
        for i in 0..4 {
            z += self.weights[i] * f_arr[i];
        }
        sigmoid(z)
    }

    pub fn predict(&self, features: &Features) -> f64 {
        let x = features.normalized.as_array();
        self.predict_features(&x)
    }

    pub fn record_prediction_step(&mut self, features: &Features) -> f64 {
        let x = features.normalized.as_array();
        let q = self.predict_features(&x);
        let flagged = q >= self.classification_threshold;

        if self.horizon_queue.len() >= 64 {
            self.horizon_queue.pop_front();
        }

        self.horizon_queue.push_back(HorizonSample {
            timestamp_us: now_us(),
            features: x,
            predicted_probability: q,
            horizon_k: self.horizon_k,
            packets_remaining: self.horizon_k,
            high_risk_flagged: flagged,
        });

        q
    }

    pub fn train_step(&mut self, features: &[f64; 4], label: f64) -> (f64, f64) {
        let label = clamp01(label);
        let q = self.predict_features(features);
        let error = label - q;

        for i in 0..4 {
            let update = self.learning_rate * error * features[i];
            self.weights[i] = (self.weights[i] + update).clamp(-5.0, 5.0);
        }
        self.bias = (self.bias + self.learning_rate * error).clamp(-5.0, 5.0);

        let eps = 1e-12;
        let q_clamped = q.clamp(eps, 1.0 - eps);
        let bce = -(label * q_clamped.ln() + (1.0 - label) * (1.0 - q_clamped).ln());

        self.metrics.total_predictions = self.metrics.total_predictions.saturating_add(1);
        self.metrics.bce_loss_sum += bce;

        let predicted_pos = q >= self.classification_threshold;
        let actual_pos = label >= 0.5;

        match (predicted_pos, actual_pos) {
            (true, true) => self.metrics.true_positives += 1,
            (true, false) => self.metrics.false_positives += 1,
            (false, false) => self.metrics.true_negatives += 1,
            (false, true) => self.metrics.false_negatives += 1,
        }

        (q, bce)
    }

    pub fn on_event(&mut self, is_congestion_loss: bool) {
        let label = if is_congestion_loss { 1.0 } else { 0.0 };
        let now = now_us();

        let mut expired = Vec::new();
        for sample in self.horizon_queue.iter_mut() {
            if is_congestion_loss && sample.high_risk_flagged && sample.timestamp_us < now {
                let lead_us = now.saturating_sub(sample.timestamp_us);
                self.metrics.lead_time_sum_us = self.metrics.lead_time_sum_us.saturating_add(lead_us as u128);
                self.metrics.lead_time_samples += 1;
                self.metrics.min_lead_time_us = Some(match self.metrics.min_lead_time_us {
                    Some(m) => m.min(lead_us),
                    None => lead_us,
                });
                self.metrics.max_lead_time_us = Some(match self.metrics.max_lead_time_us {
                    Some(m) => m.max(lead_us),
                    None => lead_us,
                });
            }

            sample.packets_remaining = sample.packets_remaining.saturating_sub(1);
            if sample.packets_remaining == 0 || is_congestion_loss {
                expired.push((sample.features, label));
            }
        }

        self.horizon_queue.retain(|s| s.packets_remaining > 0 && !is_congestion_loss);

        for (feat, lbl) in expired {
            self.train_step(&feat, lbl);
        }
    }

    pub fn status(&self) -> String {
        format!(
            "w=[{:.2},{:.2},{:.2},{:.2}] b={:.2} lr={:.3} k={} p={:.2} r={:.2} f1={:.2} bce={:.3} t_lead={}us",
            self.weights[0],
            self.weights[1],
            self.weights[2],
            self.weights[3],
            self.bias,
            self.learning_rate,
            self.horizon_k,
            self.metrics.precision(),
            self.metrics.recall(),
            self.metrics.f1_score(),
            self.metrics.avg_bce_loss(),
            self.metrics.avg_lead_time_us(),
        )
    }
}

pub type SimpleAiPredictor = OnlineLogisticPredictor;

pub struct AiCongestionController {
    features: FeatureExtractor,
    pub predictor: OnlineLogisticPredictor,

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
    pub lambda_ai_weight: f64,
}

impl AiCongestionController {
    pub fn new(mss: usize) -> Self {
        let mss = if mss == 0 { 1200 } else { mss };

        Self {
            features: FeatureExtractor::new(16),
            predictor: OnlineLogisticPredictor::new(),

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
            lambda_ai_weight: 0.60,
        }
    }

    pub fn set_lambda(&mut self, lambda: f64) {
        self.lambda_ai_weight = clamp01(lambda);
    }
}

impl CongestionController for AiCongestionController {
    fn name(&self) -> &str {
        "online-sgd-ai"
    }

    fn can_send(&self, packet_size: usize) -> bool {
        if self.in_flight_bytes == 0 {
            return true;
        }

        self.in_flight_bytes.saturating_add(packet_size) <= self.cwnd_bytes
    }

    fn on_packet_sent(&mut self, packet_size: usize) {
        self.in_flight_bytes = self.in_flight_bytes.saturating_add(packet_size);
    }

    fn on_ack(&mut self, packet_size: usize, rtt: Duration) {
        self.ack_events = self.ack_events.saturating_add(1);
        self.acks_since_reduction = self.acks_since_reduction.saturating_add(1);

        self.features.record_ack(rtt);
        let features = self.features.features();

        let ai_risk = self.predictor.record_prediction_step(&features);
        self.predictor.on_event(false);

        let heuristic_risk = features.risk;
        let combined_risk = clamp01(
            self.lambda_ai_weight * ai_risk + (1.0 - self.lambda_ai_weight) * heuristic_risk,
        );

        self.last_ai_risk = ai_risk;
        self.last_heuristic_risk = heuristic_risk;
        self.last_combined_risk = combined_risk;

        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(packet_size);

        if combined_risk >= 0.65 && self.acks_since_reduction >= 2 {
            let reduction_factor = 1.0 - (0.25 * combined_risk);
            let target_cwnd = ((self.cwnd_bytes as f64) * reduction_factor) as usize;
            self.cwnd_bytes = target_cwnd.max(self.min_cwnd);

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
            self.cwnd_bytes = self.cwnd_bytes.saturating_add(increase).min(self.max_cwnd);
        } else if combined_risk < 0.65 && self.ack_events % 8 == 0 {
            self.cwnd_bytes = self.cwnd_bytes.saturating_add(1).min(self.max_cwnd);
        }
    }

    fn on_loss(&mut self, packet_size: usize) {
        self.loss_events = self.loss_events.saturating_add(1);

        self.features.record_loss();
        let features = self.features.features();

        self.predictor.on_event(true);
        let ai_risk = self.predictor.predict(&features);
        let heuristic_risk = features.risk;
        let combined_risk = clamp01(
            self.lambda_ai_weight * ai_risk + (1.0 - self.lambda_ai_weight) * heuristic_risk,
        );

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
            "ai cwnd_bytes={} in_flight_bytes={} mss={} ai_risk={:.2} combined_risk={:.2} heuristic_risk={:.2} loss_rate={:.2} jitter_us={} trend_us={} proactive_reductions={} loss_events={} retx_events={} predictor={}",
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
            self.retransmit_events,
            self.predictor.status()
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
    fn online_sgd_learns_bce_pattern() {
        let mut predictor = OnlineLogisticPredictor::new();
        predictor.set_learning_rate(0.1);

        let high_loss_features = [0.8, 0.9, 0.7, 1.0];
        let low_loss_features = [0.0, 0.0, 0.0, 0.0];

        for _ in 0..50 {
            predictor.train_step(&high_loss_features, 1.0);
            predictor.train_step(&low_loss_features, 0.0);
        }

        let p_high = predictor.predict_features(&high_loss_features);
        let p_low = predictor.predict_features(&low_loss_features);

        assert!(p_high > p_low, "p_high ({}) must be > p_low ({})", p_high, p_low);
        assert!(p_high > 0.5, "p_high ({}) must be > 0.5", p_high);
        assert!(p_low < 0.5, "p_low ({}) must be < 0.5", p_low);
    }
}
