use std::io;
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendOutcome {
    Sent,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DynamicPattern {
    None,
    Step { threshold_packets: u64, high_loss_pct: f64 },
    Fading { period_packets: u64, min_loss_pct: f64, max_loss_pct: f64 },
    Surge { burst_start: u64, burst_len: u64, surge_loss_pct: f64 },
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x243F_6A88_85A3_08D3
        } else {
            seed
        };

        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;

        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;

        self.state = x;

        x
    }
}

pub struct NetworkEmulator {
    loss_percent: f64,
    delay_ms: u64,
    jitter_ms: u64,
    rng: XorShift64,
    packet_counter: u64,
    pattern: DynamicPattern,
}

fn now_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
}

impl NetworkEmulator {
    pub fn new() -> Self {
        Self {
            loss_percent: 0.0,
            delay_ms: 0,
            jitter_ms: 0,
            rng: XorShift64::new(now_seed()),
            packet_counter: 0,
            pattern: DynamicPattern::None,
        }
    }

    pub fn set_loss(&mut self, percent: f64) {
        let percent = if percent.is_finite() {
            percent
        } else {
            0.0
        };

        self.loss_percent = percent.clamp(0.0, 100.0);
    }

    pub fn set_delay(&mut self, ms: u64) {
        self.delay_ms = ms.min(10_000);
    }

    pub fn set_jitter(&mut self, ms: u64) {
        self.jitter_ms = ms.min(10_000);
    }

    pub fn set_pattern(&mut self, pattern: DynamicPattern) {
        self.pattern = pattern;
    }

    pub fn clear(&mut self) {
        self.loss_percent = 0.0;
        self.delay_ms = 0;
        self.jitter_ms = 0;
        self.packet_counter = 0;
        self.pattern = DynamicPattern::None;
    }

    pub fn is_active(&self) -> bool {
        self.loss_percent > 0.0
            || self.delay_ms > 0
            || self.jitter_ms > 0
            || self.pattern != DynamicPattern::None
    }

    pub fn max_delay_ms(&self) -> u64 {
        self.delay_ms.saturating_add(self.jitter_ms)
    }

    pub fn current_loss_percent(&self) -> f64 {
        match self.pattern {
            DynamicPattern::None => self.loss_percent,
            DynamicPattern::Step { threshold_packets, high_loss_pct } => {
                if self.packet_counter >= threshold_packets {
                    high_loss_pct
                } else {
                    self.loss_percent
                }
            }
            DynamicPattern::Fading { period_packets, min_loss_pct, max_loss_pct } => {
                let period = period_packets.max(1);
                let phase = (self.packet_counter % period) as f64 / period as f64;
                let sin_val = (phase * 2.0 * std::f64::consts::PI).sin().abs();
                min_loss_pct + (max_loss_pct - min_loss_pct) * sin_val
            }
            DynamicPattern::Surge { burst_start, burst_len, surge_loss_pct } => {
                if self.packet_counter >= burst_start && self.packet_counter < burst_start + burst_len {
                    surge_loss_pct
                } else {
                    self.loss_percent
                }
            }
        }
    }

    pub fn status(&self) -> String {
        format!(
            "loss={:.1}% delay={}ms jitter={}ms pattern={:?} pkts={} active={}",
            self.current_loss_percent(),
            self.delay_ms,
            self.jitter_ms,
            self.pattern,
            self.packet_counter,
            self.is_active()
        )
    }

    fn chance(&mut self, percent: f64) -> bool {
        let percent = if percent.is_finite() {
            percent
        } else {
            0.0
        };

        let percent = percent.clamp(0.0, 100.0);

        if percent <= 0.0 {
            return false;
        }

        if percent >= 100.0 {
            return true;
        }

        let value = (self.rng.next_u64() % 10_000) as f64;

        value < percent * 100.0
    }

    fn random_jitter_ms(&mut self) -> u64 {
        if self.jitter_ms == 0 {
            return 0;
        }

        self.rng.next_u64() % (self.jitter_ms + 1)
    }

    pub fn send_packet(
        &mut self,
        socket: &UdpSocket,
        data: &[u8],
    ) -> io::Result<SendOutcome> {
        self.packet_counter = self.packet_counter.saturating_add(1);

        if !self.is_active() {
            socket.send(data)?;
            return Ok(SendOutcome::Sent);
        }

        let effective_loss = self.current_loss_percent();

        if self.chance(effective_loss) {
            return Ok(SendOutcome::Dropped);
        }

        let delay = self.delay_ms.saturating_add(self.random_jitter_ms());

        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }

        socket.send(data)?;

        Ok(SendOutcome::Sent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emulator_default_status() {
        let emulator = NetworkEmulator::new();

        assert!(emulator.status().contains("loss=0.0%"));
        assert!(emulator.status().contains("delay=0ms"));
        assert!(emulator.status().contains("jitter=0ms"));
        assert!(emulator.status().contains("active=false"));
    }

    #[test]
    fn step_pattern_changes_loss() {
        let mut em = NetworkEmulator::new();
        em.set_loss(5.0);
        em.set_pattern(DynamicPattern::Step { threshold_packets: 10, high_loss_pct: 30.0 });

        assert_eq!(em.current_loss_percent(), 5.0);
        em.packet_counter = 12;
        assert_eq!(em.current_loss_percent(), 30.0);
    }
}
