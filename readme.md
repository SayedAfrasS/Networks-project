# Adaptive Transport Prototype

This is a research prototype for an adaptive transport protocol.

It supports:

- Adaptive reliability classes
- Best effort, important, and guaranteed delivery
- ACK and retransmission
- Simple congestion control
- Priority scheduling
- Telemetry logging
- Metrics collection

## Run Server

```powershell
cargo run -- server 127.0.0.1:9000