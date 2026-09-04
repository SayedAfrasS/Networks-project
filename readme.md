# Adaptive transport prototype

A research prototype of an adaptive transport protocol built on UDP. Written in Rust with no external dependencies (`#![forbid(unsafe_code)]`, pure `std`).

The prototype explores three ideas: per-packet reliability classes, multiple congestion control strategies (including a small online-learning model), and simulated multipath steering. It includes a built-in network emulator so you can test everything without separate tooling.

## Building and running

Requires Rust 1.56+ (edition 2021).

```
cargo build
cargo test
```

Start a server:

```
cargo run -- server 127.0.0.1:9000
```

Start the interactive client:

```
cargo run -- client 127.0.0.1:9000
```

## How it works

### Packet format

Every packet has a fixed 28-byte header:

| Field | Size | Description |
|---|---:|---|
| version | 1 byte | Protocol version (currently 1) |
| packet_type | 1 byte | DATA, ACK, SYN, SYN_ACK, CLOSE, CLOSE_ACK |
| reliability | 1 byte | BestEffort, Important, or Guaranteed |
| priority | 1 byte | Application-assigned priority (0-255) |
| connection_id | 4 bytes | Connection identifier |
| stream_id | 4 bytes | Stream identifier |
| sequence_number | 4 bytes | Packet sequence number |
| acknowledgment_number | 4 bytes | ACKed sequence number |
| timestamp_us | 8 bytes | Sender timestamp in microseconds |

Payload follows immediately after the header.

### Reliability classes

Each packet carries one of three reliability levels:

- `BestEffort`: fire and forget, no ACK, no retransmission.
- `Important`: requires ACK, up to 3 retransmissions.
- `Guaranteed`: requires ACK, up to 8 retransmissions.

### Priority scheduling

Packets are dequeued by priority (higher value goes first). Equal priority breaks ties by sequence number (lower first).

### Connection lifecycle

Connections follow a SYN/SYN_ACK handshake and CLOSE/CLOSE_ACK teardown. Each connection can carry multiple streams, and each stream has its own reliability class, priority, and per-stream statistics (sent, acked, lost, retransmitted, in-flight, average RTT).

### Congestion control

Three controllers are included. You can switch between them at runtime.

`simple-aimd` is a baseline AIMD controller. It increases the congestion window by one packet on each ACK and halves it on loss.

`predictive-risk` uses a heuristic risk score computed from RTT trend, RTT inflation, jitter, and loss rate (weighted 0.35, 0.25, 0.20, 0.20). When risk exceeds 0.65 it proactively reduces the window. When risk falls below 0.35 it grows the window.

`simple-ai` adds a 4-weight sigmoid predictor trained online with SGD. The predictor takes normalized trend, inflation, jitter, and loss as input. Its output is blended 60/40 with the heuristic risk score. Loss events and retransmissions provide training labels. The controller adjusts the window based on the combined risk, using the same 0.65/0.35 thresholds.

### Feature extraction

The feature extractor maintains a sliding window (default 16 samples) of RTT measurements and packet outcomes. It computes: latest RTT, average RTT, min/max RTT, RTT trend, jitter, loss rate, and a composite congestion risk score. These features feed into both the predictive and AI controllers.

### Multipath

The prototype simulates two network paths:

- Path 0: wifi (initial quality 0.90)
- Path 1: cellular (initial quality 0.70)

Traffic is steered based on reliability and priority. Guaranteed packets go to the best path. BestEffort packets go to the secondary path. Important packets go to the best path if priority >= 5, otherwise the secondary path.

Path quality adjusts dynamically: +0.02 on each ACK, -0.15 on loss, -0.05 on retransmit. Each path has its own network emulator instance.

### Network emulation

The built-in emulator can apply configurable packet loss (%), delay (ms), and jitter (ms). It uses a xorshift64 PRNG seeded from the system clock. Preset scenarios:

- `good`: no impairment
- `lossy`: 10% loss / 10ms delay / 5ms jitter on wifi, 30% / 80ms / 50ms on cellular
- `mixed`: clean wifi, degraded cellular (30% loss / 120ms / 80ms)
- `bad`: 35% loss / 150ms delay / 100ms jitter on both paths

## Client commands

Once connected, the interactive client accepts these commands:

| Command | What it does |
|---|---|
| `connect` | Open a connection (SYN handshake) |
| `close` | Close the connection (CLOSE handshake) |
| `send <text>` | Send data on the current stream |
| `mkstream <reliability> <priority>` | Create a new stream (e.g. `mkstream guaranteed 5`) |
| `use <id>` | Switch to a stream by ID |
| `streams` | List all streams |
| `streamstats` | Print per-stream statistics |
| `stats` | Print global metrics |
| `summary` | Aggregate experiment CSV into summary CSV |
| `mpsummary` | Aggregate multipath CSV into summary CSV |
| `controller` | Show the active congestion controller |
| `aimd` | Switch to simple-aimd |
| `predictive` | Switch to predictive-risk |
| `ai` | Switch to simple-ai |
| `reset` | Reset metrics and congestion state |
| `loss <percent>` | Set emulated loss rate |
| `delay <ms>` | Set emulated delay |
| `jitter <ms>` | Set emulated jitter |
| `scenario <name>` | Apply a preset scenario (good, lossy, mixed, bad) |
| `multipath` | Toggle multipath on/off |
| `experiment <n>` | Run n experiment iterations and log results |
| `batch <n>` | Run experiments across all controllers and scenarios |
| `netinfo` | Show current emulator status |
| `quit` / `exit` | Exit the client |

## Experiment framework

The `experiment` command sends a batch of packets across all three reliability classes, waits for ACKs and timeouts, then logs the results to `experiment_results.csv`. When multipath is enabled, per-path statistics go to `multipath_results.csv`. The `summary` and `mpsummary` commands aggregate these CSVs into `summary_results.csv` and `multipath_summary.csv`.

Telemetry events (sends, ACKs, losses, retransmits, controller changes) are logged to `telemetry.jsonl` in a one-JSON-object-per-line format.

## Benchmarking

PowerShell scripts automate benchmark runs:

- `benchmark.ps1`: runs all three controllers against good/lossy/bad scenarios, writes experiment CSV and summary CSV.
- `benchmark_multipath.ps1`: same thing with multipath enabled.
- `baseline_iperf3.ps1`: baseline comparison using iperf3.

Python scripts for analysis:

- `analyze_summary.py`: reads summary CSV and prints a formatted table.
- `make_graphs.py`: generates comparison charts from experiment data.
- `stats_report.py`: computes statistical summaries from experiment CSV.

## Project structure

```
src/
  main.rs          Entry point, server and client modes
  packet.rs        Packet encoding/decoding, reliability enum
  congestion.rs    AIMD and predictive controllers
  ai.rs            Online-learning congestion controller
  features.rs      RTT/loss feature extraction
  scheduler.rs     Priority-based packet scheduler
  multipath.rs     Multipath manager and path selection
  emulator.rs      Network emulator (loss, delay, jitter)
  connection.rs    Connection and stream management
  metrics.rs       Global send/ack/loss/RTT counters
  experiment.rs    Experiment runner and CSV logger
  report.rs        Experiment summary aggregation
  mp_report.rs     Multipath summary aggregation
  telemetry.rs     JSONL telemetry logger
```

## Protocol specification

See [PROTOCOL.md](PROTOCOL.md) for the full protocol spec and [STATE_MACHINE.md](STATE_MACHINE.md) for the formal state machines (connection, stream, retransmission, multipath path) and their invariants.

## License

Not specified. This is a research prototype.
