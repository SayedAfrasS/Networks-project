use std::cmp::Ordering;
use std::io;
use std::net::UdpSocket;
use std::time::Duration;

use crate::emulator::{NetworkEmulator, SendOutcome};
use crate::packet::Reliability;

#[derive(Default)]
pub struct MultipathStats {
    pub acks: u64,
    pub losses: u64,
    pub retransmits: u64,
    pub rtt_samples: u64,
    pub rtt_sum_us: u128,
}

pub struct ManagedPath {
    pub id: u8,
    pub name: String,
    pub enabled: bool,
    pub quality: f64,
    pub emulator: NetworkEmulator,
    pub stats: MultipathStats,
}

impl ManagedPath {
    fn new(id: u8, name: &str, quality: f64) -> Self {
        Self {
            id,
            name: name.to_string(),
            enabled: true,
            quality: quality.clamp(0.0, 1.0),
            emulator: NetworkEmulator::new(),
            stats: MultipathStats::default(),
        }
    }

    pub fn max_delay_ms(&self) -> u64 {
        self.emulator.max_delay_ms()
    }

    pub fn status(&self) -> String {
        format!(
            "{}[id={}] q={:.2} acks={} losses={} retx={} {}",
            self.name,
            self.id,
            self.quality,
            self.stats.acks,
            self.stats.losses,
            self.stats.retransmits,
            self.emulator.status()
        )
    }
}

pub struct MultipathManager {
    pub enabled: bool,
    pub paths: Vec<ManagedPath>,
}

impl MultipathManager {
    pub fn new() -> Self {
        Self {
            enabled: false,
            paths: vec![
                ManagedPath::new(0, "wifi", 0.90),
                ManagedPath::new(1, "cellular", 0.70),
            ],
        }
    }

    pub fn status(&self) -> String {
        let state = if self.enabled { "enabled" } else { "disabled" };

        let path_statuses: Vec<String> =
            self.paths.iter().map(|p| p.status()).collect();

        format!("multipath={} {}", state, path_statuses.join(" | "))
    }

    pub fn set_scenario(&mut self, scenario: &str) {
        match scenario {
            "good" => {
                for path in &mut self.paths {
                    path.emulator.clear();
                }
            }
            "lossy" => {
                if let Some(path) = self.paths.get_mut(0) {
                    path.emulator.set_loss(10.0);
                    path.emulator.set_delay(10);
                    path.emulator.set_jitter(5);
                }

                if let Some(path) = self.paths.get_mut(1) {
                    path.emulator.set_loss(30.0);
                    path.emulator.set_delay(80);
                    path.emulator.set_jitter(50);
                }
            }
            "mixed" => {
                if let Some(path) = self.paths.get_mut(0) {
                    path.emulator.clear();
                }

                if let Some(path) = self.paths.get_mut(1) {
                    path.emulator.set_loss(30.0);
                    path.emulator.set_delay(120);
                    path.emulator.set_jitter(80);
                }
            }
            "bad" => {
                for path in &mut self.paths {
                    path.emulator.set_loss(35.0);
                    path.emulator.set_delay(150);
                    path.emulator.set_jitter(100);
                }
            }
            _ => {}
        }
    }

    fn best_path_id(&self) -> u8 {
        self.paths
            .iter()
            .filter(|p| p.enabled)
            .max_by(|a, b| {
                a.quality
                    .partial_cmp(&b.quality)
                    .unwrap_or(Ordering::Equal)
            })
            .map(|p| p.id)
            .unwrap_or(0)
    }

    fn secondary_path_id(&self) -> u8 {
        let mut enabled: Vec<&ManagedPath> =
            self.paths.iter().filter(|p| p.enabled).collect();

        if enabled.is_empty() {
            return 0;
        }

        enabled.sort_by(|a, b| {
            b.quality
                .partial_cmp(&a.quality)
                .unwrap_or(Ordering::Equal)
        });

        if enabled.len() > 1 {
            enabled[1].id
        } else {
            enabled[0].id
        }
    }

    pub fn choose_path(&self, reliability: Reliability, priority: u8) -> u8 {
        if !self.enabled {
            return 0;
        }

        match reliability {
            Reliability::BestEffort => self.secondary_path_id(),
            Reliability::Important => {
                if priority >= 5 {
                    self.best_path_id()
                } else {
                    self.secondary_path_id()
                }
            }
            Reliability::Guaranteed => self.best_path_id(),
        }
    }

    pub fn path_max_delay_ms(&self, path_id: u8) -> u64 {
        self.paths
            .iter()
            .find(|p| p.id == path_id)
            .map(|p| p.max_delay_ms())
            .unwrap_or(0)
    }

    pub fn send_packet(
        &mut self,
        path_id: u8,
        socket: &UdpSocket,
        data: &[u8],
    ) -> io::Result<SendOutcome> {
        let index = self
            .paths
            .iter()
            .position(|p| p.id == path_id && p.enabled)
            .or_else(|| self.paths.iter().position(|p| p.enabled));

        match index {
            Some(index) => self.paths[index].emulator.send_packet(socket, data),
            None => {
                socket.send(data)?;
                Ok(SendOutcome::Sent)
            }
        }
    }

    pub fn on_ack(&mut self, path_id: u8, rtt: Duration) {
        if let Some(path) = self.paths.iter_mut().find(|p| p.id == path_id) {
            path.stats.acks = path.stats.acks.saturating_add(1);
            path.stats.rtt_samples = path.stats.rtt_samples.saturating_add(1);
            path.stats.rtt_sum_us = path
                .stats
                .rtt_sum_us
                .saturating_add(rtt.as_micros());

            path.quality = (path.quality + 0.02).clamp(0.0, 1.0);
        }
    }

    pub fn on_loss(&mut self, path_id: u8) {
        if let Some(path) = self.paths.iter_mut().find(|p| p.id == path_id) {
            path.stats.losses = path.stats.losses.saturating_add(1);

            path.quality = (path.quality - 0.15).clamp(0.05, 1.0);
        }
    }

    pub fn on_retransmit(&mut self, path_id: u8) {
        if let Some(path) = self.paths.iter_mut().find(|p| p.id == path_id) {
            path.stats.retransmits = path.stats.retransmits.saturating_add(1);

            path.quality = (path.quality - 0.05).clamp(0.05, 1.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_chooses_valid_path() {
        let mut manager = MultipathManager::new();

        manager.enabled = true;

        let critical_path =
            manager.choose_path(Reliability::Guaranteed, 7);

        let best_effort_path =
            manager.choose_path(Reliability::BestEffort, 0);

        assert!(critical_path == 0 || critical_path == 1);
        assert!(best_effort_path == 0 || best_effort_path == 1);
    }
}