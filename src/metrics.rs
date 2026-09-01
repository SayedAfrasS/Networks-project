use std::time::Duration;

use crate::packet::Reliability;

pub struct Metrics {
    pub sent_total: u64,
    pub sent_best_effort: u64,
    pub sent_important: u64,
    pub sent_guaranteed: u64,

    pub acks: u64,
    pub losses: u64,
    pub retransmits: u64,

    pub rtt_samples: u64,
    pub rtt_sum_us: u128,
    pub rtt_min_us: Option<u128>,
    pub rtt_max_us: Option<u128>,

    pub priority_counts: [u64; 256],
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            sent_total: 0,
            sent_best_effort: 0,
            sent_important: 0,
            sent_guaranteed: 0,

            acks: 0,
            losses: 0,
            retransmits: 0,

            rtt_samples: 0,
            rtt_sum_us: 0,
            rtt_min_us: None,
            rtt_max_us: None,

            priority_counts: [0; 256],
        }
    }

    pub fn record_send(&mut self, reliability: Reliability, priority: u8) {
        self.sent_total = self.sent_total.saturating_add(1);

        match reliability {
            Reliability::BestEffort => {
                self.sent_best_effort = self.sent_best_effort.saturating_add(1);
            }
            Reliability::Important => {
                self.sent_important = self.sent_important.saturating_add(1);
            }
            Reliability::Guaranteed => {
                self.sent_guaranteed = self.sent_guaranteed.saturating_add(1);
            }
        }

        self.priority_counts[priority as usize] =
            self.priority_counts[priority as usize].saturating_add(1);
    }

    pub fn record_ack(&mut self, rtt: Duration) {
        self.acks = self.acks.saturating_add(1);

        self.rtt_samples = self.rtt_samples.saturating_add(1);

        let rtt_us = rtt.as_micros();

        self.rtt_sum_us = self.rtt_sum_us.saturating_add(rtt_us);

        self.rtt_min_us = Some(match self.rtt_min_us {
            Some(current_min) => current_min.min(rtt_us),
            None => rtt_us,
        });

        self.rtt_max_us = Some(match self.rtt_max_us {
            Some(current_max) => current_max.max(rtt_us),
            None => rtt_us,
        });
    }

    pub fn record_loss(&mut self) {
        self.losses = self.losses.saturating_add(1);
    }

    pub fn record_retransmit(&mut self) {
        self.retransmits = self.retransmits.saturating_add(1);
    }

    pub fn summary(&self) -> String {
        let rtt_avg_us = if self.rtt_samples == 0 {
            0
        } else {
            self.rtt_sum_us / self.rtt_samples as u128
        };

        let rtt_min = self
            .rtt_min_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "0".to_string());

        let rtt_max = self
            .rtt_max_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "0".to_string());

        let mut priority_summary = String::new();

        for (priority, count) in self.priority_counts.iter().enumerate() {
            if *count > 0 {
                priority_summary.push_str(&format!("p{}={} ", priority, count));
            }
        }

        if priority_summary.is_empty() {
            priority_summary = "none".to_string();
        }

        format!(
            "sent_total={} be={} important={} guaranteed={} acks={} losses={} retransmits={} rtt_samples={} rtt_avg_us={} rtt_min_us={} rtt_max_us={} priorities={}",
            self.sent_total,
            self.sent_best_effort,
            self.sent_important,
            self.sent_guaranteed,
            self.acks,
            self.losses,
            self.retransmits,
            self.rtt_samples,
            rtt_avg_us,
            rtt_min,
            rtt_max,
            priority_summary.trim()
        )
    }
}