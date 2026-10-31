# Documentation & Scripts Analysis

## 1. readme.md

### Summary
Project README for "Adaptive Reliability and Congestion-Aware Multipath Transport over Heterogeneous UDP Paths." Documents the full prototype: building, running, packet format, reliability classes, priority scheduling, congestion control, feature extraction, multipath, network emulation, client commands, experiment framework, benchmarking, and project structure.

### What it does
Serves as the primary entry point documentation for the project.

### Key Parameters & Constants
- **Packet header**: fixed 28-byte header (version, packet_type, reliability, priority, connection_id, stream_id, sequence_number, acknowledgment_number, timestamp_us)
- **Reliability classes**: BestEffort (0 retries), Important (up to 3), Guaranteed (up to 8)
- **Congestion controllers**: simple-aimd, predictive-risk, simple-ai
- **Predictive risk weights**: 0.35 RTT trend, 0.25 RTT inflation, 0.20 jitter, 0.20 loss rate
- **Thresholds**: risk > 0.65 → reduce window; risk < 0.35 → grow window
- **AI blend**: 60% AI predictor / 40% heuristic risk
- **Multipath paths**: path 0 (wifi, quality 0.90), path 1 (cellular, quality 0.70)
- **Path quality adjustments**: +0.02 ACK, -0.15 loss, -0.05 retransmit
- **Emulator scenarios**: good (no impairment), lossy (10%/10ms/5ms wifi, 30%/80ms/50ms cellular), mixed (clean wifi, degraded cellular), bad (35%/150ms/100ms both)
- **Feature window**: 16 samples default
- **Build**: Rust 1.56+, edition 2021, zero dependencies, `#![forbid(unsafe_code)]`

### Research Relevance
- Documents all existing mechanisms that map to spec sections §3–§17
- Lists all client commands required by §40
- Already describes the three congestion controllers (§9, §10, §16)
- Multipath steering policy matches §15 baseline description
- Missing: no mention of adaptive reliability (§12), joint decision engine (§11), deadline support (§18), utility function (§11), ablation (§20), or formal math model (§4–§8)

---

## 2. PROTOCOL.md

### Summary
Formal protocol specification document. Covers packet header format, packet types (DATA=0, ACK=1, SYN=2, SYN_ACK=3, CLOSE=4, CLOSE_ACK=5), reliability classes, priority scheduling, congestion controllers, feature extraction, multipath, streams, and experiment outputs.

### What it does
Serves as the protocol specification reference. More concise than readme.md — a spec document rather than a guide.

### Key Parameters & Constants
- Same header format as readme (28 bytes)
- Same six packet types
- Same three reliability classes
- Same three congestion controllers
- Features extracted: latest RTT, avg RTT, min/max RTT, RTT trend, jitter, loss rate, congestion risk score
- Two simulated paths (wifi, cellular)
- Stream properties: stream_id, reliability, priority, sent/ack/loss/retransmit counts, in-flight, avg RTT
- Outputs: telemetry.jsonl, experiment_results.csv, summary_results.csv, multipath_results.csv, multipath_summary.csv

### Research Relevance
- Needs updating for §12 (Adaptive reliability class), §11 (joint utility function), §18 (deadline fields), §27 (expanded telemetry events)
- Will need new packet header fields or extension mechanism for deadline support
- Feature extraction list (§7) is partially covered — missing RTT inflation as a distinct named feature in the spec doc (though computed in code)

---

## 3. STATE_MACHINE.md

### Summary
Formal state machines for the four main protocol components.

### What it does
Documents the state transitions for: Connection (Closed→WaitSynAck→Established→WaitCloseAck→Closed), Stream (Idle→Active→Closed), Retransmission (Ready→InFlight→Acked), Multipath Path (Available↔Degraded). Also states two formal invariants: streams can't be Active if Connection is Closed; packets can't be InFlight if Stream is Closed.

### Key Parameters & Constants
- 4 state machines, 2 invariants
- Connection: 4 states, 4 transitions
- Stream: 3 states, 3 transitions
- Retransmission: 3 states, 3 transitions
- Multipath path: 2 states, 2 transitions

### Research Relevance
- Needs extension for §12: Retransmission SM needs an "Abandoned" state for adaptive reliability budget decisions
- Needs extension for §15: Path SM could add more granular quality states
- Needs extension for §18: deadline-related states (DeadlineExpired)
- Current SMs are clean and correct for the existing system — good foundation

---

## 4. Cargo.toml

### Summary
```toml
[package]
name = "adaptive_transport"
version = "0.1.0"
edition = "2021"

[dependencies]
```

### What it does
Defines the Rust package. Zero external dependencies — pure `std` implementation.

### Key Parameters & Constants
- Name: adaptive_transport
- Version: 0.1.0
- Edition: 2021
- No dependencies

### Research Relevance
- Per §39, maintain Rust 2021 edition
- Per §7, do not use external ML libraries — currently satisfied with zero deps
- The `src/bin/spec.rs` binary is not declared as `[[bin]]` — it may be a standalone spec generator

---

## 5. .gitignore

### Summary
Ignores: `/target`, `telemetry.jsonl`, `experiment_results.csv`, `summary_results.csv`, `multipath_results.csv`, `multipath_summary.csv`, `*.png`, `stats_report.csv`, `graph_data.csv`, `*.txt`

### What it does
Keeps generated/benchmark artifacts out of version control.

### Key Parameters & Constants
- All CSV experiment outputs are gitignored
- All PNG graph outputs are gitignored
- Telemetry JSONL is gitignored
- `.txt` files (benchmark command scripts) are gitignored

### Research Relevance
- Per §26 (reproducibility): experiment CSVs are not version-controlled. This is fine as long as they can be regenerated. The benchmark scripts provide regeneration capability.
- May need to add new gitignore entries for ablation output files, stats_report outputs, etc.

---

## 6. analyze_summary.py

### Summary
Reads `summary_results.csv` and prints selected columns as CSV to stdout.

### What it does
Simple CSV column extractor. Reads the summary file and outputs: controller, emulator, runs, success_rate, weighted_avg_rtt_us, retransmits, losses, avg_final_cwnd_bytes, avg_final_risk.

### Key Parameters & Constants
- Input: `summary_results.csv`
- Output columns: controller, emulator, runs, success_rate, weighted_avg_rtt_us, retransmits, losses, avg_final_cwnd_bytes, avg_final_risk
- No statistical computation — just column extraction

### Research Relevance
- Very basic — does not compute any of the statistical measures required by §23 (confidence intervals, effect sizes, significance tests)
- Will need replacement or significant extension for research-grade analysis
- Maps to §29 Table 3 (average performance) partially — but without CI or statistical tests

---

## 7. stats_report.py

### Summary
Statistical analysis script. Reads `experiment_results.csv` and `multipath_results.csv`, groups data by controller/scenario, computes per-group statistics (n, mean, std, min, max, 95% CI), and writes `stats_report.csv`.

### What it does
1. Groups experiment rows by `controller / scenario` (e.g., "simple-aimd / single good")
2. Groups multipath rows by `controller / path` (e.g., "simple-ai / path wifi")
3. For each group, computes per-metric statistics: n, mean, sample stdev, min, max, 95% CI
4. Experiment metrics: success_percent, rtt_ms, retransmits, losses, duration_ms
5. Multipath metrics: rtt_ms, retransmits, losses, quality
6. Outputs `stats_report.csv` and prints formatted summary

### Key Parameters & Constants
- CI uses z=1.96 (normal approximation, not t-distribution as specified in §23)
- Scenario labels derived from emulator string: "loss=0.0%" → good, "loss=20.0%" → lossy, "loss=35.0%" → bad
- Groups: controller × scenario for experiment; controller × path for multipath

### Research Relevance
- Partially satisfies §23 (statistical analysis): has mean, std, CI95
- Missing from §23: median, t-distribution CI (uses z=1.96 instead of t-critical), effect sizes, paired statistical tests, significance testing
- Missing from §22: p95 RTT, p99 RTT, goodput, delivery ratio, cwnd evolution, recovery time, per-class delivery probability
- Good foundation — needs extension rather than rewrite
- Does NOT support the ablation study format (§20)

---

## 8. make_graphs.py

### Summary
Graph generation script. Reads `summary_results.csv`, writes `graph_data.csv`, and generates bar charts using matplotlib (optional dependency).

### What it does
1. Loads summary rows, converts to labeled data points ("controller / network_label")
2. Writes a flat `graph_data.csv` with: label, success_percent, avg_rtt_ms, retransmits, losses, avg_final_cwnd_bytes, avg_final_risk, avg_duration_ms
3. Generates 6 bar charts: success_rate.png, avg_rtt.png, retransmits.png, losses.png, final_risk.png, duration.png
4. Falls back gracefully if matplotlib is not installed

### Key Parameters & Constants
- Figure size: 13×5 inches
- DPI: 200
- Charts generated: success rate, avg RTT, retransmissions, losses, final risk, duration
- matplotlib is the only external Python dependency (optional)

### Research Relevance
- Partially satisfies §28 (required visualizations): has some transport metrics charted
- Missing from §28: system architecture diagram (Fig 1), decision pipeline (Fig 2), prediction lead time (Fig 3), throughput vs loss (Fig 4), p95 RTT vs loss (Fig 5), reliability achieved vs requested (Fig 7), single vs multipath (Fig 8), path utilization (Fig 9), ablation (Fig 10), cwnd over time (Fig 11), prediction behavior (Fig 12), real vs emulator (Fig 13)
- Current charts are basic bar charts — not publication quality
- No confidence interval error bars
- No grouped/faceted comparisons
- Needs significant extension for the 13+ required figures

---

## 9. benchmark.ps1

### Summary
PowerShell script that automates single-path benchmark runs.

### What it does
1. Builds the project with `cargo build`
2. Starts the server on `127.0.0.1:9000`
3. Generates a command sequence: connect → for each controller (aimd, predictive, ai) × each scenario (good, lossy, bad): run `experiment $Runs` → summary → close → exit
4. Pipes commands to the client via stdin
5. Cleans up server process

### Key Parameters & Constants
- Default runs: 5 per configuration
- Controllers tested: aimd, predictive, ai (3)
- Scenarios tested: good, lossy, bad (3)
- Total configs: 3 × 3 = 9
- Total experiment runs: 9 × 5 = 45 (default)
- Server address: 127.0.0.1:9000
- Startup wait: 3 seconds

### Research Relevance
- Partially satisfies §19 (experiment design): covers 3 controllers × 3 scenarios
- Missing from §19: Adaptive controller, joint-adaptive controller, multipath factor, priority factor, deadline factor, adaptive reliability
- §19 requires 30 runs per config (default); current default is 5
- Missing: deterministic seeds (§26), experiment_id/run_id tracking, full reproducibility metadata
- Good foundation for automation — needs extension to cover the full factorial design
- Does not support ablation (§20)

---

## 10. benchmark_multipath.ps1

### Summary
PowerShell script that automates multipath benchmark runs.

### What it does
1. Same build/server setup as benchmark.ps1
2. Enables multipath, then for each controller (aimd, predictive, ai) × each multipath scenario (good/good, mixed/lossy, bad/bad): runs `experiment $Runs`
3. Generates both summary and mpsummary
4. Outputs: experiment_results.csv, summary_results.csv, multipath_results.csv, multipath_summary.csv

### Key Parameters & Constants
- Default runs: 5 per configuration
- Controllers: aimd, predictive, ai (3)
- Multipath scenarios: good, mixed, bad (3)
- Total configs: 3 × 3 = 9
- Total runs: 45 (default)
- Uses both `multipath <scenario>` and `scenario <scenario>` commands together

### Research Relevance
- Extends benchmark.ps1 with multipath dimension
- Partially satisfies §19 multipath factor
- Same gaps as benchmark.ps1: missing adaptive controller, insufficient runs, no seeds, no ablation
- Combined with benchmark.ps1: covers 18 configurations (9 single + 9 multipath)
- §19 full factorial would require significantly more configurations

---

## Cross-Cutting Gaps Summary

| Research Requirement | Current Coverage | Gap |
|---|---|---|
| §4 Formal system model | Not documented | Need MATHEMATICAL_MODEL.md |
| §5 Feature normalization | Not documented, partial in code | Need formal normalization with configurable constants |
| §6 Congestion-risk model | Partially in code (ai.rs) | Need formal sigmoid predictor documentation |
| §7 Online learning | In code (ai.rs) | Need formal SGD documentation |
| §8 Prediction lead time | Not implemented | Need instrumentation |
| §12 Adaptive reliability | Not implemented | Need new reliability mode |
| §11 Joint utility function | Not implemented | Need utility.rs / adaptive.rs |
| §18 Deadline support | Not implemented | Need deadline field + urgency |
| §19 Full experiment design | Partial (9+9 configs) | Need full factorial + 30 runs |
| §20 Ablation | Not implemented | Need A0–A8 framework |
| §23 Statistics | Partial (mean/std/CI) | Need median, t-dist CI, effect sizes, tests |
| §24 Dynamic emulator | Not implemented | Need time-varying scenarios |
| §26 Reproducibility | Partial (CSV) | Need seeds, experiment_id, full metadata |
| §28 Visualizations | 6 basic charts | Need 13+ publication figures |
| §29 Tables | Not implemented | Need 7 research tables |
| §31 Literature review | Not started | Need related-work matrix |
| §43 Documentation | README + PROTOCOL + STATE_MACHINE | Need RESEARCH.md, MATHEMATICAL_MODEL.md, EXPERIMENTS.md, RELATED_WORK.md, REPRODUCIBILITY.md |
