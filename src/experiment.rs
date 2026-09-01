use std::convert::TryFrom;
use std::fs::{metadata, OpenOptions};
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ExperimentResult {
    pub timestamp_us: u64,
    pub run_id: u32,

    pub controller: String,
    pub emulator: String,

    pub sent_total: u64,
    pub sent_best_effort: u64,
    pub sent_important: u64,
    pub sent_guaranteed: u64,

    pub acks: u64,
    pub losses: u64,
    pub retransmits: u64,

    pub rtt_samples: u64,
    pub rtt_avg_us: u128,
    pub rtt_min_us: u128,
    pub rtt_max_us: u128,

    pub final_cwnd_bytes: usize,
    pub final_risk: f64,

    pub duration_ms: u128,
}

impl ExperimentResult {
    pub fn summary_short(&self) -> String {
        format!(
            "sent={} acks={} losses={} retx={} avg_rtt_us={} cwnd={} risk={:.2}",
            self.sent_total,
            self.acks,
            self.losses,
            self.retransmits,
            self.rtt_avg_us,
            self.final_cwnd_bytes,
            self.final_risk
        )
    }
}

pub fn timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn csv_escape(value: &str) -> String {
    if value.contains(',')
        || value.contains('"')
        || value.contains('\n')
        || value.contains('\r')
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub struct ExperimentLogger {
    path: String,
}

impl ExperimentLogger {
    pub fn new(path: &str) -> io::Result<Self> {
        let needs_header = match metadata(path) {
            Ok(meta) => meta.len() == 0,
            Err(_) => true,
        };

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        if needs_header {
            writeln!(
                file,
                "timestamp_us,run_id,controller,emulator,sent_total,sent_best_effort,sent_important,sent_guaranteed,acks,losses,retransmits,rtt_samples,rtt_avg_us,rtt_min_us,rtt_max_us,final_cwnd_bytes,final_risk,duration_ms"
            )?;

            file.flush()?;
        }

        Ok(Self {
            path: path.to_string(),
        })
    }

    pub fn write_result(&mut self, result: &ExperimentResult) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{}",
            result.timestamp_us,
            result.run_id,
            csv_escape(&result.controller),
            csv_escape(&result.emulator),
            result.sent_total,
            result.sent_best_effort,
            result.sent_important,
            result.sent_guaranteed,
            result.acks,
            result.losses,
            result.retransmits,
            result.rtt_samples,
            result.rtt_avg_us,
            result.rtt_min_us,
            result.rtt_max_us,
            result.final_cwnd_bytes,
            result.final_risk,
            result.duration_ms
        )?;

        file.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_escape_handles_commas() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hello\""), "\"say \"\"hello\"\"\"");
    }
}