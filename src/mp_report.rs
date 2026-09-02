use std::collections::BTreeMap;
use std::fs::{metadata, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};

use crate::experiment::timestamp_us;
use crate::multipath::MultipathManager;

pub struct MultipathExperimentLogger {
    path: String,
}

impl MultipathExperimentLogger {
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
                "timestamp_us,run_id,controller,path_id,path_name,acks,losses,retransmits,rtt_samples,rtt_avg_us,quality"
            )?;

            file.flush()?;
        }

        Ok(Self {
            path: path.to_string(),
        })
    }

    pub fn write_run(
        &mut self,
        run_id: u32,
        controller: &str,
        manager: &MultipathManager,
    ) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        let ts = timestamp_us();

        for path in &manager.paths {
            let rtt_avg_us = if path.stats.rtt_samples > 0 {
                path.stats.rtt_sum_us / path.stats.rtt_samples as u128
            } else {
                0
            };

            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{},{:.6}",
                ts,
                run_id,
                csv_escape(controller),
                path.id,
                csv_escape(&path.name),
                path.stats.acks,
                path.stats.losses,
                path.stats.retransmits,
                path.stats.rtt_samples,
                rtt_avg_us,
                path.quality
            )?;
        }

        file.flush()?;

        Ok(())
    }
}

pub struct MultipathSummaryRow {
    pub controller: String,
    pub path_id: u8,
    pub path_name: String,

    pub runs: u64,

    pub acks: u64,
    pub losses: u64,
    pub retransmits: u64,

    pub weighted_avg_rtt_us: u128,
    pub avg_quality: f64,
}

impl MultipathSummaryRow {
    pub fn short(&self) -> String {
        format!(
            "controller={} path={} name={} runs={} acks={} losses={} retx={} avg_rtt_us={} quality={:.2}",
            self.controller,
            self.path_id,
            self.path_name,
            self.runs,
            self.acks,
            self.losses,
            self.retransmits,
            self.weighted_avg_rtt_us,
            self.avg_quality
        )
    }
}

#[derive(Default)]
struct Agg {
    runs: u64,

    acks: u64,
    losses: u64,
    retransmits: u64,

    rtt_weighted_sum: u128,
    rtt_samples: u64,

    quality_sum: f64,
}

fn parse_u64(value: &str) -> u64 {
    value.trim().parse::<u64>().unwrap_or(0)
}

fn parse_u128(value: &str) -> u128 {
    value.trim().parse::<u128>().unwrap_or(0)
}

fn parse_f64(value: &str) -> f64 {
    let parsed = value.trim().parse::<f64>().unwrap_or(0.0);

    if parsed.is_finite() {
        parsed
    } else {
        0.0
    }
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

fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' if !in_quotes => {
                in_quotes = true;
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    fields.push(current);

    fields
}

pub fn summarize_multipath_csv(
    input_path: &str,
    output_path: &str,
) -> io::Result<Vec<MultipathSummaryRow>> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);

    let mut groups: BTreeMap<(String, u8, String), Agg> = BTreeMap::new();

    for (line_index, line_result) in reader.lines().enumerate() {
        let line = line_result?;

        if line_index == 0 {
            continue;
        }

        if line.trim().is_empty() {
            continue;
        }

        let cols = split_csv_line(&line);

        if cols.len() < 11 {
            continue;
        }

        let controller = cols[2].trim().to_string();
        let path_id = parse_u64(&cols[3]) as u8;
        let path_name = cols[4].trim().to_string();

        let acks = parse_u64(&cols[5]);
        let losses = parse_u64(&cols[6]);
        let retransmits = parse_u64(&cols[7]);

        let rtt_samples = parse_u64(&cols[8]);
        let rtt_avg_us = parse_u128(&cols[9]);

        let quality = parse_f64(&cols[10]);

        let entry = groups
            .entry((controller, path_id, path_name))
            .or_insert_with(Agg::default);

        entry.runs = entry.runs.saturating_add(1);

        entry.acks = entry.acks.saturating_add(acks);
        entry.losses = entry.losses.saturating_add(losses);
        entry.retransmits = entry.retransmits.saturating_add(retransmits);

        entry.rtt_samples = entry.rtt_samples.saturating_add(rtt_samples);

        entry.rtt_weighted_sum = entry
            .rtt_weighted_sum
            .saturating_add(rtt_avg_us.saturating_mul(rtt_samples as u128));

        entry.quality_sum += quality;
    }

    let mut rows = Vec::new();

    for ((controller, path_id, path_name), agg) in groups {
        let weighted_avg_rtt_us = if agg.rtt_samples > 0 {
            agg.rtt_weighted_sum / agg.rtt_samples as u128
        } else {
            0
        };

        let avg_quality = if agg.runs > 0 {
            agg.quality_sum / agg.runs as f64
        } else {
            0.0
        };

        rows.push(MultipathSummaryRow {
            controller,
            path_id,
            path_name,

            runs: agg.runs,

            acks: agg.acks,
            losses: agg.losses,
            retransmits: agg.retransmits,

            weighted_avg_rtt_us,
            avg_quality,
        });
    }

    let mut output_file = File::create(output_path)?;

    writeln!(
        output_file,
        "controller,path_id,path_name,runs,acks,losses,retransmits,weighted_avg_rtt_us,avg_quality"
    )?;

    for row in &rows {
        writeln!(
            output_file,
            "{},{},{},{},{},{},{},{},{:.6}",
            csv_escape(&row.controller),
            row.path_id,
            csv_escape(&row.path_name),
            row.runs,
            row.acks,
            row.losses,
            row.retransmits,
            row.weighted_avg_rtt_us,
            row.avg_quality
        )?;
    }

    output_file.flush()?;

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_csv_handles_quotes() {
        let line = "a,\"b,c\",d";

        let cols = split_csv_line(line);

        assert_eq!(
            cols,
            vec![
                "a".to_string(),
                "b,c".to_string(),
                "d".to_string()
            ]
        );
    }
}