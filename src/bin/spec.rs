use std::fmt::Write as _;
use std::fs;
use std::io;
use std::process;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnState {
    Closed,
    WaitSynAck,
    Established,
    WaitCloseAck,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnEvent {
    Connect,
    SynAckReceived,
    Close,
    CloseAckReceived,

    #[allow(dead_code)]
    Timeout,
}

struct ConnStateMachine {
    state: ConnState,
    log: Vec<String>,
}

impl ConnStateMachine {
    fn new() -> Self {
        Self {
            state: ConnState::Closed,
            log: Vec::new(),
        }
    }

    fn apply(&mut self, event: ConnEvent) -> Result<(), String> {
        let old_state = self.state;

        let next_state = match (self.state, event) {
            (ConnState::Closed, ConnEvent::Connect) => ConnState::WaitSynAck,

            (ConnState::WaitSynAck, ConnEvent::SynAckReceived) => ConnState::Established,

            (ConnState::WaitSynAck, ConnEvent::Timeout) => ConnState::Closed,

            (ConnState::Established, ConnEvent::Close) => ConnState::WaitCloseAck,

            (ConnState::WaitCloseAck, ConnEvent::CloseAckReceived) => ConnState::Closed,

            (ConnState::WaitCloseAck, ConnEvent::Timeout) => ConnState::Closed,

            _ => {
                return Err(format!(
                    "invalid transition from {:?} on {:?}",
                    old_state,
                    event
                ))
            }
        };

        self.state = next_state;

        self.log.push(format!(
            "{:?} --{:?}--> {:?}",
            old_state,
            event,
            next_state
        ));

        Ok(())
    }
}

fn run_self_check() -> Result<ConnStateMachine, String> {
    let mut machine = ConnStateMachine::new();

    machine.apply(ConnEvent::Connect)?;
    machine.apply(ConnEvent::SynAckReceived)?;
    machine.apply(ConnEvent::Close)?;
    machine.apply(ConnEvent::CloseAckReceived)?;

    Ok(machine)
}

fn protocol_spec() -> String {
    let mut out = String::new();

    writeln!(out, "# Adaptive Transport Prototype Protocol Specification").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 1. Overview").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "This prototype implements an adaptive transport protocol over UDP."
    )
    .unwrap();
    writeln!(
        out,
        "It supports adaptive reliability, priority scheduling, congestion control,"
    )
    .unwrap();
    writeln!(
        out,
        "feature extraction, AI-assisted congestion prediction, multipath steering,"
    )
    .unwrap();
    writeln!(
        out,
        "network emulation, streams, connection management, and experiment reporting."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 2. Packet Header Format").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Fixed header size: 28 bytes.").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Field | Size | Description |").unwrap();
    writeln!(out, "|---|---:|---|").unwrap();
    writeln!(out, "| version | 1 byte | Protocol version |").unwrap();
    writeln!(out, "| packet_type | 1 byte | DATA, ACK, SYN, SYN_ACK, CLOSE, CLOSE_ACK |").unwrap();
    writeln!(out, "| reliability | 1 byte | BestEffort, Important, Guaranteed |").unwrap();
    writeln!(out, "| priority | 1 byte | Application priority |").unwrap();
    writeln!(out, "| connection_id | 4 bytes | Connection identifier |").unwrap();
    writeln!(out, "| stream_id | 4 bytes | Stream identifier |").unwrap();
    writeln!(out, "| sequence_number | 4 bytes | Packet sequence number |").unwrap();
    writeln!(out, "| acknowledgment_number | 4 bytes | ACKed sequence number |").unwrap();
    writeln!(out, "| timestamp_us | 8 bytes | Sender timestamp in microseconds |").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 3. Packet Types").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Type | Value | Meaning |").unwrap();
    writeln!(out, "|---|---:|---|").unwrap();
    writeln!(out, "| DATA | 0 | Application data packet |").unwrap();
    writeln!(out, "| ACK | 1 | Acknowledgment for reliable data |").unwrap();
    writeln!(out, "| SYN | 2 | Connection open request |").unwrap();
    writeln!(out, "| SYN_ACK | 3 | Connection open response |").unwrap();
    writeln!(out, "| CLOSE | 4 | Connection close request |").unwrap();
    writeln!(out, "| CLOSE_ACK | 5 | Connection close response |").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 4. Reliability Classes").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Class | Behavior |").unwrap();
    writeln!(out, "|---|---|").unwrap();
    writeln!(out, "| BestEffort | No ACK, no retransmission |").unwrap();
    writeln!(out, "| Important | ACK required, limited retransmissions |").unwrap();
    writeln!(out, "| Guaranteed | ACK required, more retransmissions |").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 5. Priority Scheduling").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Packets are scheduled by priority. Higher priority packets are sent first."
    )
    .unwrap();
    writeln!(
        out,
        "If priority is equal, lower sequence number is sent first."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 6. Congestion Controllers").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Controller | Description |").unwrap();
    writeln!(out, "|---|---|").unwrap();
    writeln!(out, "| simple-aimd | Baseline additive-increase multiplicative-decrease controller |").unwrap();
    writeln!(out, "| predictive-risk | Heuristic predictive controller using RTT trend, jitter, and loss |").unwrap();
    writeln!(out, "| simple-ai | Online learning controller using a small adaptive model |").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 7. Feature Extraction").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "The protocol extracts:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- latest RTT").unwrap();
    writeln!(out, "- average RTT").unwrap();
    writeln!(out, "- minimum RTT").unwrap();
    writeln!(out, "- maximum RTT").unwrap();
    writeln!(out, "- RTT trend").unwrap();
    writeln!(out, "- jitter").unwrap();
    writeln!(out, "- loss rate").unwrap();
    writeln!(out, "- congestion risk score").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 8. Multipath Support").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "The prototype simulates two paths:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- path 0: wifi").unwrap();
    writeln!(out, "- path 1: cellular").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "High-priority reliable traffic is steered to the better path."
    )
    .unwrap();
    writeln!(
        out,
        "Best-effort traffic may use the secondary path."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 9. Streams").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Each connection can contain multiple streams.").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Each stream has:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- stream ID").unwrap();
    writeln!(out, "- reliability class").unwrap();
    writeln!(out, "- priority").unwrap();
    writeln!(out, "- sent count").unwrap();
    writeln!(out, "- ACK count").unwrap();
    writeln!(out, "- loss count").unwrap();
    writeln!(out, "- retransmission count").unwrap();
    writeln!(out, "- in-flight count").unwrap();
    writeln!(out, "- average RTT").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## 10. Experiment Outputs").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "The prototype can generate:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- telemetry.jsonl").unwrap();
    writeln!(out, "- experiment_results.csv").unwrap();
    writeln!(out, "- summary_results.csv").unwrap();
    writeln!(out, "- multipath_results.csv").unwrap();
    writeln!(out, "- multipath_summary.csv").unwrap();
    writeln!(out, "- PROTOCOL.md").unwrap();
    writeln!(out, "- STATE_MACHINE.md").unwrap();
    writeln!(out).unwrap();

    out
}

fn state_machine_spec(machine: &ConnStateMachine) -> String {
    let mut out = String::new();

    writeln!(out, "# Connection State Machine").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## States").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- Closed").unwrap();
    writeln!(out, "- WaitSynAck").unwrap();
    writeln!(out, "- Established").unwrap();
    writeln!(out, "- WaitCloseAck").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Events").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- Connect").unwrap();
    writeln!(out, "- SynAckReceived").unwrap();
    writeln!(out, "- Close").unwrap();
    writeln!(out, "- CloseAckReceived").unwrap();
    writeln!(out, "- Timeout").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Valid Transitions").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```text").unwrap();
    writeln!(out, "Closed --Connect--> WaitSynAck").unwrap();
    writeln!(out, "WaitSynAck --SynAckReceived--> Established").unwrap();
    writeln!(out, "WaitSynAck --Timeout--> Closed").unwrap();
    writeln!(out, "Established --Close--> WaitCloseAck").unwrap();
    writeln!(out, "WaitCloseAck --CloseAckReceived--> Closed").unwrap();
    writeln!(out, "WaitCloseAck --Timeout--> Closed").unwrap();
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Self-Check Trace").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```text").unwrap();

    for entry in &machine.log {
        writeln!(out, "{}", entry).unwrap();
    }

    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Stream State").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Each stream is currently modeled with simple states:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- created").unwrap();
    writeln!(out, "- active").unwrap();
    writeln!(out, "- closed when connection closes").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Retransmission State").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Each reliable packet has:").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- first send time").unwrap();
    writeln!(out, "- last send time").unwrap();
    writeln!(out, "- attempt count").unwrap();
    writeln!(out, "- maximum attempts").unwrap();
    writeln!(out, "- path ID").unwrap();
    writeln!(out, "- stream ID").unwrap();
    writeln!(out).unwrap();

    out
}

fn main() -> io::Result<()> {
    let machine = match run_self_check() {
        Ok(machine) => machine,
        Err(e) => {
            eprintln!("State machine self-check failed: {}", e);
            process::exit(1);
        }
    };

    fs::write("PROTOCOL.md", protocol_spec())?;
    fs::write("STATE_MACHINE.md", state_machine_spec(&machine))?;

    println!("State machine self-check passed.");
    println!("Wrote PROTOCOL.md");
    println!("Wrote STATE_MACHINE.md");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_connection_lifecycle() {
        let mut machine = ConnStateMachine::new();

        assert!(machine.apply(ConnEvent::Connect).is_ok());
        assert!(machine.apply(ConnEvent::SynAckReceived).is_ok());
        assert!(machine.apply(ConnEvent::Close).is_ok());
        assert!(machine.apply(ConnEvent::CloseAckReceived).is_ok());
    }

    #[test]
    fn invalid_transition_fails() {
        let mut machine = ConnStateMachine::new();

        assert!(machine.apply(ConnEvent::Close).is_err());
    }
}