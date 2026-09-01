use std::io;
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendOutcome {
    Sent,
    Dropped,
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

    pub fn clear(&mut self) {
        self.loss_percent = 0.0;
        self.delay_ms = 0;
        self.jitter_ms = 0;
    }

    pub fn is_active(&self) -> bool {
        self.loss_percent > 0.0 || self.delay_ms > 0 || self.jitter_ms > 0
    }

    pub fn max_delay_ms(&self) -> u64 {
        self.delay_ms.saturating_add(self.jitter_ms)
    }

    pub fn status(&self) -> String {
        format!(
            "loss={:.1}% delay={}ms jitter={}ms active={}",
            self.loss_percent,
            self.delay_ms,
            self.jitter_ms,
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
        if !self.is_active() {
            socket.send(data)?;
            return Ok(SendOutcome::Sent);
        }

        let delay = self.delay_ms.saturating_add(self.random_jitter_ms());

        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }

        if self.chance(self.loss_percent) {
            return Ok(SendOutcome::Dropped);
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
}