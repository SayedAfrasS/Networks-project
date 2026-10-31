use std::cmp::Ordering;
use crate::packet::Reliability;

#[derive(Debug, Clone, Copy)]
pub struct JatdeWeights {
    pub alpha_deadline: f64,
    pub beta_stream: f64,
    pub gamma_reliability: f64,
    pub delta_quality: f64,
    pub mu_cost: f64,
}

impl Default for JatdeWeights {
    fn default() -> Self {
        Self {
            alpha_deadline: 0.30,
            beta_stream: 0.20,
            gamma_reliability: 0.25,
            delta_quality: 0.15,
            mu_cost: 0.10,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PacketContext {
    pub priority: u8,
    pub created_us: u64,
    pub deadline_us: u64,
    pub size_bytes: usize,
    pub retries: u32,
    pub max_retries: u32,
}

impl PacketContext {
    pub fn new(priority: u8, size_bytes: usize) -> Self {
        Self {
            priority,
            created_us: 0,
            deadline_us: 0,
            size_bytes,
            retries: 0,
            max_retries: 5,
        }
    }

    pub fn with_deadline(mut self, created_us: u64, deadline_us: u64) -> Self {
        self.created_us = created_us;
        self.deadline_us = deadline_us;
        self
    }

    pub fn deadline_decay(&self, now_us: u64) -> f64 {
        if self.deadline_us == 0 {
            return 1.0;
        }
        if now_us <= self.created_us {
            return 1.0;
        }
        let elapsed = now_us.saturating_sub(self.created_us);
        if elapsed >= self.deadline_us {
            0.0
        } else {
            1.0 - (elapsed as f64 / self.deadline_us as f64)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PathQualityInfo {
    pub path_id: u8,
    pub quality: f64,
    pub loss_rate: f64,
    pub rtt_ms: f64,
    pub cost_multiplier: f64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct StreamHealthInfo {
    pub stream_id: u32,
    pub priority: u8,
    pub sent_count: u64,
    pub acked_count: u64,
}

impl StreamHealthInfo {
    pub fn success_ratio(&self) -> f64 {
        if self.sent_count == 0 {
            1.0
        } else {
            (self.acked_count as f64 / self.sent_count as f64).clamp(0.0, 1.0)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Decision {
    pub path_id: u8,
    pub concrete_reliability: Reliability,
    pub utility_score: f64,
}

#[derive(Debug, Clone)]
pub struct JatdeEngine {
    pub weights: JatdeWeights,
    pub retransmit_threshold: f64,
    pub decisions_made: u64,
    pub proactive_drops: u64,
    pub total_utility_sum: f64,
}

impl JatdeEngine {
    pub fn new() -> Self {
        Self {
            weights: JatdeWeights::default(),
            retransmit_threshold: 0.15,
            decisions_made: 0,
            proactive_drops: 0,
            total_utility_sum: 0.0,
        }
    }

    pub fn reliability_val(mode: Reliability) -> f64 {
        match mode {
            Reliability::BestEffort => 0.20,
            Reliability::Important => 0.70,
            Reliability::Guaranteed => 1.00,
            Reliability::Adaptive => 0.60,
        }
    }

    pub fn compute_cost(
        packet_size: usize,
        path: &PathQualityInfo,
        mode: Reliability,
    ) -> f64 {
        let l_k = path.loss_rate.clamp(0.0, 0.90);
        let retry_factor = match mode {
            Reliability::BestEffort => 1.0,
            Reliability::Important => 1.0 + l_k,
            Reliability::Guaranteed => 1.0 + (l_k / (1.0 - l_k).max(0.10)),
            Reliability::Adaptive => 1.0 + (0.5 * l_k),
        };
        let footprint = (packet_size as f64) * retry_factor * path.cost_multiplier;
        let c_max = 1400.0 * 4.0;
        (footprint / c_max).clamp(0.0, 1.0)
    }

    pub fn compute_utility(
        &self,
        pkt: &PacketContext,
        stream: &StreamHealthInfo,
        path: &PathQualityInfo,
        mode: Reliability,
        now_us: u64,
    ) -> f64 {
        let v_i = (pkt.priority as f64) / 255.0;
        let d_i = pkt.deadline_decay(now_us);
        let p_i = (stream.priority as f64) / 255.0;
        let s_i = stream.success_ratio();
        let rel_val = Self::reliability_val(mode);
        let q_k = path.quality.clamp(0.0, 1.0);
        let cost = Self::compute_cost(pkt.size_bytes, path, mode);

        let u = self.weights.alpha_deadline * (v_i * d_i)
            + self.weights.beta_stream * (p_i * s_i)
            + self.weights.gamma_reliability * rel_val
            + self.weights.delta_quality * q_k
            - self.weights.mu_cost * cost;

        u.clamp(-1.0, 2.0)
    }

    pub fn select_optimal(
        &mut self,
        pkt: &PacketContext,
        stream: &StreamHealthInfo,
        paths: &[PathQualityInfo],
        requested_mode: Reliability,
        now_us: u64,
    ) -> Decision {
        let candidate_modes: &[Reliability] = match requested_mode {
            Reliability::Adaptive => &[
                Reliability::BestEffort,
                Reliability::Important,
                Reliability::Guaranteed,
            ],
            specific => &[specific],
        };

        let mut best_decision = Decision {
            path_id: 0,
            concrete_reliability: match requested_mode {
                Reliability::Adaptive => Reliability::Important,
                m => m,
            },
            utility_score: -999.0,
        };

        for path in paths.iter().filter(|p| p.enabled) {
            for &mode in candidate_modes {
                let u = self.compute_utility(pkt, stream, path, mode, now_us);
                if u > best_decision.utility_score {
                    best_decision.utility_score = u;
                    best_decision.path_id = path.path_id;
                    best_decision.concrete_reliability = mode;
                }
            }
        }

        self.decisions_made = self.decisions_made.saturating_add(1);
        self.total_utility_sum += best_decision.utility_score.max(0.0);

        best_decision
    }

    pub fn should_retransmit(
        &mut self,
        pkt: &PacketContext,
        stream: &StreamHealthInfo,
        best_path: &PathQualityInfo,
        mode: Reliability,
        now_us: u64,
    ) -> bool {
        if pkt.retries >= pkt.max_retries {
            return false;
        }

        if mode == Reliability::Guaranteed {
            return true;
        }

        let d_i = pkt.deadline_decay(now_us);
        if d_i <= 0.0 {
            self.proactive_drops = self.proactive_drops.saturating_add(1);
            return false;
        }

        let u_retransmit = self.compute_utility(pkt, stream, best_path, mode, now_us);
        let expected_gain = u_retransmit * d_i - (0.05 * (pkt.retries as f64 + 1.0));

        if expected_gain >= self.retransmit_threshold {
            true
        } else {
            self.proactive_drops = self.proactive_drops.saturating_add(1);
            false
        }
    }

    pub fn avg_utility(&self) -> f64 {
        if self.decisions_made == 0 {
            0.0
        } else {
            self.total_utility_sum / self.decisions_made as f64
        }
    }

    pub fn status(&self) -> String {
        format!(
            "jatde decisions={} avg_u={:.3} proactive_drops={} w=[a={:.2},b={:.2},g={:.2},d={:.2},m={:.2}] tau={:.2}",
            self.decisions_made,
            self.avg_utility(),
            self.proactive_drops,
            self.weights.alpha_deadline,
            self.weights.beta_stream,
            self.weights.gamma_reliability,
            self.weights.delta_quality,
            self.weights.mu_cost,
            self.retransmit_threshold,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jatde_deadline_decay_works() {
        let pkt = PacketContext::new(100, 500).with_deadline(1000, 2000);
        assert_eq!(pkt.deadline_decay(1000), 1.0);
        assert_eq!(pkt.deadline_decay(2000), 0.5);
        assert_eq!(pkt.deadline_decay(3000), 0.0);
        assert_eq!(pkt.deadline_decay(4000), 0.0);
    }

    #[test]
    fn jatde_selects_higher_quality_path() {
        let mut engine = JatdeEngine::new();
        let pkt = PacketContext::new(200, 1000);
        let stream = StreamHealthInfo {
            stream_id: 1,
            priority: 200,
            sent_count: 10,
            acked_count: 10,
        };

        let paths = vec![
            PathQualityInfo {
                path_id: 0,
                quality: 0.95,
                loss_rate: 0.01,
                rtt_ms: 10.0,
                cost_multiplier: 1.0,
                enabled: true,
            },
            PathQualityInfo {
                path_id: 1,
                quality: 0.30,
                loss_rate: 0.25,
                rtt_ms: 80.0,
                cost_multiplier: 1.5,
                enabled: true,
            },
        ];

        let decision = engine.select_optimal(&pkt, &stream, &paths, Reliability::Adaptive, 5000);
        assert_eq!(decision.path_id, 0);
        assert!(decision.utility_score > 0.0);
    }

    #[test]
    fn jatde_drops_expired_deadline_on_retransmit() {
        let mut engine = JatdeEngine::new();
        let pkt = PacketContext::new(100, 500).with_deadline(1000, 1000); // expired at 2000
        let stream = StreamHealthInfo {
            stream_id: 1,
            priority: 100,
            sent_count: 5,
            acked_count: 4,
        };
        let path = PathQualityInfo {
            path_id: 0,
            quality: 0.8,
            loss_rate: 0.05,
            rtt_ms: 20.0,
            cost_multiplier: 1.0,
            enabled: true,
        };

        let should_retry = engine.should_retransmit(&pkt, &stream, &path, Reliability::Important, 2500);
        assert!(!should_retry);
        assert_eq!(engine.proactive_drops, 1);
    }
}
