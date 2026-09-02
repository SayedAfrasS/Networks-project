# Adaptive Transport Prototype Protocol Specification

## 1. Overview

This prototype implements an adaptive transport protocol over UDP.
It supports adaptive reliability, priority scheduling, congestion control,
feature extraction, AI-assisted congestion prediction, multipath steering,
network emulation, streams, connection management, and experiment reporting.

## 2. Packet Header Format

Fixed header size: 28 bytes.

| Field | Size | Description |
|---|---:|---|
| version | 1 byte | Protocol version |
| packet_type | 1 byte | DATA, ACK, SYN, SYN_ACK, CLOSE, CLOSE_ACK |
| reliability | 1 byte | BestEffort, Important, Guaranteed |
| priority | 1 byte | Application priority |
| connection_id | 4 bytes | Connection identifier |
| stream_id | 4 bytes | Stream identifier |
| sequence_number | 4 bytes | Packet sequence number |
| acknowledgment_number | 4 bytes | ACKed sequence number |
| timestamp_us | 8 bytes | Sender timestamp in microseconds |

## 3. Packet Types

| Type | Value | Meaning |
|---|---:|---|
| DATA | 0 | Application data packet |
| ACK | 1 | Acknowledgment for reliable data |
| SYN | 2 | Connection open request |
| SYN_ACK | 3 | Connection open response |
| CLOSE | 4 | Connection close request |
| CLOSE_ACK | 5 | Connection close response |

## 4. Reliability Classes

| Class | Behavior |
|---|---|
| BestEffort | No ACK, no retransmission |
| Important | ACK required, limited retransmissions |
| Guaranteed | ACK required, more retransmissions |

## 5. Priority Scheduling

Packets are scheduled by priority. Higher priority packets are sent first.
If priority is equal, lower sequence number is sent first.

## 6. Congestion Controllers

| Controller | Description |
|---|---|
| simple-aimd | Baseline additive-increase multiplicative-decrease controller |
| predictive-risk | Heuristic predictive controller using RTT trend, jitter, and loss |
| simple-ai | Online learning controller using a small adaptive model |

## 7. Feature Extraction

The protocol extracts:

- latest RTT
- average RTT
- minimum RTT
- maximum RTT
- RTT trend
- jitter
- loss rate
- congestion risk score

## 8. Multipath Support

The prototype simulates two paths:

- path 0: wifi
- path 1: cellular

High-priority reliable traffic is steered to the better path.
Best-effort traffic may use the secondary path.

## 9. Streams

Each connection can contain multiple streams.

Each stream has:

- stream ID
- reliability class
- priority
- sent count
- ACK count
- loss count
- retransmission count
- in-flight count
- average RTT

## 10. Experiment Outputs

The prototype can generate:

- telemetry.jsonl
- experiment_results.csv
- summary_results.csv
- multipath_results.csv
- multipath_summary.csv
- PROTOCOL.md
- STATE_MACHINE.md

