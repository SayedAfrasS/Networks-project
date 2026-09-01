use std::collections::VecDeque;
use std::convert::TryFrom;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Features {
    pub samples: usize,

    pub latest_rtt_us: u64,
    pub avg_rtt_us: u64,
    pub min_rtt_us: u64,
    pub max_rtt_us: u64,

    pub rtt_trend_us: i64,
    pub jitter_us: u64,

    pub loss_rate: f64,
    pub risk: f64,
}

pub struct FeatureExtractor {
    window_size: usize,
    rtt_window: VecDeque<u64>,
    outcome_window: VecDeque<bool>,
}

fn clamp01(value: f64) -> f64 {
    if value < 0.0 {
        0.0
    } else if value > 1.0 {
        1.0
    } else {
        value
    }
}

impl FeatureExtractor {
    pub fn new(window_size: usize) -> Self {
        let window_size = if window_size == 0 { 16 } else { window_size };

        Self {
            window_size,
            rtt_window: VecDeque::with_capacity(window_size),
            outcome_window: VecDeque::with_capacity(window_size),
        }
    }

    pub fn record_ack(&mut self, rtt: Duration) {
        let rtt_us = u64::try_from(rtt.as_micros()).unwrap_or(u64::MAX);

        self.rtt_window.push_back(rtt_us);

        if self.rtt_window.len() > self.window_size {
            self.rtt_window.pop_front();
        }

        self.outcome_window.push_back(true);

        if self.outcome_window.len() > self.window_size {
            self.outcome_window.pop_front();
        }
    }

    pub fn record_loss(&mut self) {
        self.outcome_window.push_back(false);

        if self.outcome_window.len() > self.window_size {
            self.outcome_window.pop_front();
        }
    }

    fn loss_rate(&self) -> f64 {
        if self.outcome_window.is_empty() {
            return 0.0;
        }

        let losses = self
            .outcome_window
            .iter()
            .filter(|ok| !**ok)
            .count();

        losses as f64 / self.outcome_window.len() as f64
    }

    pub fn features(&self) -> Features {
        let samples = self.rtt_window.len();

        let loss_rate = self.loss_rate();

        if samples == 0 {
            let risk = clamp01(loss_rate * 2.0);

            return Features {
                samples: 0,
                latest_rtt_us: 0,
                avg_rtt_us: 0,
                min_rtt_us: 0,
                max_rtt_us: 0,
                rtt_trend_us: 0,
                jitter_us: 0,
                loss_rate,
                risk,
            };
        }

        let mut sum: u128 = 0;
        let mut min_rtt_us = u64::MAX;
        let mut max_rtt_us = 0;

        for rtt in &self.rtt_window {
            sum = sum.saturating_add(*rtt as u128);

            if *rtt < min_rtt_us {
                min_rtt_us = *rtt;
            }

            if *rtt > max_rtt_us {
                max_rtt_us = *rtt;
            }
        }

        let avg_rtt_us = (sum / samples as u128) as u64;

        let latest_rtt_us = *self.rtt_window.back().unwrap_or(&0);

        let first_rtt_us = *self.rtt_window.front().unwrap_or(&0);

        let rtt_trend_us = if samples >= 2 {
            let trend = (latest_rtt_us as i128 - first_rtt_us as i128)
                / (samples as i128 - 1);

            trend as i64
        } else {
            0
        };

        let jitter_us = if samples >= 2 {
            let mut diff_sum: u128 = 0;
            let mut previous = self.rtt_window[0];

            for (i, current) in self.rtt_window.iter().enumerate() {
                if i == 0 {
                    continue;
                }

                let diff = if *current > previous {
                    *current - previous
                } else {
                    previous - *current
                };

                diff_sum = diff_sum.saturating_add(diff as u128);

                previous = *current;
            }

            (diff_sum / (samples as u128 - 1)) as u64
        } else {
            0
        };

        let avg_for_risk = avg_rtt_us.max(1000) as f64;

        let trend_risk = if rtt_trend_us > 0 {
            clamp01(rtt_trend_us as f64 / avg_for_risk)
        } else {
            0.0
        };

        let inflation_risk = if min_rtt_us > 0 && latest_rtt_us > min_rtt_us {
            clamp01((latest_rtt_us as f64 / min_rtt_us as f64) - 1.0)
        } else {
            0.0
        };

        let jitter_risk = clamp01(jitter_us as f64 / avg_for_risk);

        let loss_risk = clamp01(loss_rate * 2.0);

        let risk = clamp01(
            0.35 * trend_risk
                + 0.25 * inflation_risk
                + 0.20 * jitter_risk
                + 0.20 * loss_risk,
        );

        Features {
            samples,
            latest_rtt_us,
            avg_rtt_us,
            min_rtt_us,
            max_rtt_us,
            rtt_trend_us,
            jitter_us,
            loss_rate,
            risk,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extractor_computes_basic_features() {
        let mut extractor = FeatureExtractor::new(8);

        extractor.record_ack(Duration::from_micros(100));
        extractor.record_ack(Duration::from_micros(300));
        extractor.record_loss();

        let features = extractor.features();

        assert_eq!(features.samples, 2);
        assert_eq!(features.latest_rtt_us, 300);
        assert_eq!(features.min_rtt_us, 100);
        assert_eq!(features.max_rtt_us, 300);

        assert!(features.loss_rate > 0.0);
        assert!(features.jitter_us > 0);
    }
}