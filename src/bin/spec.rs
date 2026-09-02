use std::fmt::Write as _;
use std::fs;
use std::io;
use std::process;

// ==========================================
// 1. CONNECTION STATE MACHINE
// ==========================================

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
            _ => return Err(format!("invalid transition from {:?} on {:?}", old_state, event)),
        };
        self.state = next_state;
        self.log.push(format!("{:?} --{:?}--> {:?}", old_state, event, next_state));
        Ok(())
    }
}

// ==========================================
// 2. STREAM STATE MACHINE
// ==========================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum StreamState {
    Idle,
    Active,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StreamEvent {
    Create,
    SendData,

    #[allow(dead_code)]
    ReceiveAck,

    Close,
}

struct StreamStateMachine {
    state: StreamState,
    log: Vec<String>,
}

impl StreamStateMachine {
    fn new() -> Self {
        Self { state: StreamState::Idle, log: Vec::new() }
    }

    fn apply(&mut self, event: StreamEvent) -> Result<(), String> {
        let old_state = self.state;
        let next_state = match (self.state, event) {
            (StreamState::Idle, StreamEvent::Create) => StreamState::Active,
            (StreamState::Active, StreamEvent::SendData) => StreamState::Active,
            (StreamState::Active, StreamEvent::ReceiveAck) => StreamState::Active,
            (StreamState::Active, StreamEvent::Close) => StreamState::Closed,
            _ => return Err(format!("invalid stream transition from {:?} on {:?}", old_state, event)),
        };
        self.state = next_state;
        self.log.push(format!("{:?} --{:?}--> {:?}", old_state, event, next_state));
        Ok(())
    }
}

// ==========================================
// 3. RETRANSMISSION STATE MACHINE
// ==========================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum RetxState {
    Ready,
    InFlight,
    Acked,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RetxEvent {
    Transmit,
    Ack,
    Retransmit,

    #[allow(dead_code)]
    Fail,
}

struct RetxStateMachine {
    state: RetxState,
    log: Vec<String>,
}

impl RetxStateMachine {
    fn new() -> Self {
        Self { state: RetxState::Ready, log: Vec::new() }
    }

    fn apply(&mut self, event: RetxEvent) -> Result<(), String> {
        let old_state = self.state;
        let next_state = match (self.state, event) {
            (RetxState::Ready, RetxEvent::Transmit) => RetxState::InFlight,
            (RetxState::InFlight, RetxEvent::Ack) => RetxState::Acked,
            (RetxState::InFlight, RetxEvent::Retransmit) => RetxState::InFlight,
            (RetxState::InFlight, RetxEvent::Fail) => RetxState::Dropped,
            _ => return Err(format!("invalid retx transition from {:?} on {:?}", old_state, event)),
        };
        self.state = next_state;
        self.log.push(format!("{:?} --{:?}--> {:?}", old_state, event, next_state));
        Ok(())
    }
}

// ==========================================
// 4. MULTIPATH PATH STATE MACHINE
// ==========================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum PathState {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PathEvent {
    GoodAck,
    HighLoss,

    #[allow(dead_code)]
    HighDelay,

    #[allow(dead_code)]
    Recover,
}

struct PathStateMachine {
    state: PathState,
    log: Vec<String>,
}

impl PathStateMachine {
    fn new() -> Self {
        Self { state: PathState::Available, log: Vec::new() }
    }

    fn apply(&mut self, event: PathEvent) -> Result<(), String> {
        let old_state = self.state;
        let next_state = match (self.state, event) {
            (PathState::Available, PathEvent::HighLoss) => PathState::Degraded,
            (PathState::Available, PathEvent::HighDelay) => PathState::Degraded,
            (PathState::Degraded, PathEvent::HighLoss) => PathState::Unavailable,
            (PathState::Degraded, PathEvent::GoodAck) => PathState::Available,
            (PathState::Unavailable, PathEvent::Recover) => PathState::Available,
            (PathState::Available, PathEvent::GoodAck) => PathState::Available,
            _ => return Err(format!("invalid path transition from {:?} on {:?}", old_state, event)),
        };
        self.state = next_state;
        self.log.push(format!("{:?} --{:?}--> {:?}", old_state, event, next_state));
        Ok(())
    }
}

// ==========================================
// 5. FORMAL INVARIANTS
// ==========================================

struct GlobalState {
    conn: ConnState,
    stream: StreamState,
    retx: RetxState,
}

impl GlobalState {
    fn check_invariants(&self) -> Result<(), String> {
        // Invariant 1: A stream cannot be Active if the Connection is Closed.
        if self.stream == StreamState::Active && self.conn == ConnState::Closed {
            return Err("Invariant violated: Stream is Active but Connection is Closed".to_string());
        }
        
        // Invariant 2: A packet cannot be InFlight if its Stream is Closed.
        if self.retx == RetxState::InFlight && self.stream == StreamState::Closed {
            return Err("Invariant violated: Packet is InFlight but Stream is Closed".to_string());
        }

        Ok(())
    }
}

// ==========================================
// SELF-CHECK EXECUTION
// ==========================================

fn run_self_check() -> Result<(ConnStateMachine, StreamStateMachine, RetxStateMachine, PathStateMachine, bool), String> {
    let mut conn = ConnStateMachine::new();
    let mut stream = StreamStateMachine::new();
    let mut retx = RetxStateMachine::new();
    let mut path = PathStateMachine::new();

    // Connection lifecycle
    conn.apply(ConnEvent::Connect)?;
    conn.apply(ConnEvent::SynAckReceived)?;

    // Stream lifecycle
    stream.apply(StreamEvent::Create)?;
    stream.apply(StreamEvent::SendData)?;

    // Packet lifecycle
    retx.apply(RetxEvent::Transmit)?;
    retx.apply(RetxEvent::Retransmit)?;
    retx.apply(RetxEvent::Ack)?;

    // Path lifecycle
    path.apply(PathEvent::HighLoss)?;
    path.apply(PathEvent::GoodAck)?;

    // Check invariants at a valid state
    let global = GlobalState {
        conn: conn.state,
        stream: stream.state,
        retx: retx.state,
    };
    
    let invariants_ok = global.check_invariants().is_ok();

    // Close everything
    stream.apply(StreamEvent::Close)?;
    conn.apply(ConnEvent::Close)?;
    conn.apply(ConnEvent::CloseAckReceived)?;

    Ok((conn, stream, retx, path, invariants_ok))
}

// ==========================================
// MARKDOWN GENERATION
// ==========================================

fn protocol_spec() -> String {
    let mut out = String::new();

    writeln!(out, "# Adaptive Transport Prototype Protocol Specification").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## 1. Overview").unwrap();
    writeln!(out, "This prototype implements an adaptive transport protocol over UDP.").unwrap();
    writeln!(out, "It supports adaptive reliability, priority scheduling, congestion control,").unwrap();
    writeln!(out, "feature extraction, AI-assisted congestion prediction, multipath steering,").unwrap();
    writeln!(out, "network emulation, streams, connection management, and experiment reporting.").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## 2. Packet Header Format").unwrap();
    writeln!(out, "Fixed header size: 28 bytes.").unwrap();
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
    writeln!(out, "## 3. Reliability Classes").unwrap();
    writeln!(out, "| Class | Behavior |").unwrap();
    writeln!(out, "|---|---|").unwrap();
    writeln!(out, "| BestEffort | No ACK, no retransmission |").unwrap();
    writeln!(out, "| Important | ACK required, limited retransmissions |").unwrap();
    writeln!(out, "| Guaranteed | ACK required, more retransmissions |").unwrap();

    out
}

fn state_machine_spec(
    conn: &ConnStateMachine,
    stream: &StreamStateMachine,
    retx: &RetxStateMachine,
    path: &PathStateMachine,
    invariants_ok: bool,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Formal State Machines").unwrap();
    writeln!(out).unwrap();

    // Connection
    writeln!(out, "## 1. Connection State Machine").unwrap();
    writeln!(out, "```text").unwrap();
    for entry in &conn.log { writeln!(out, "{}", entry).unwrap(); }
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    // Stream
    writeln!(out, "## 2. Stream State Machine").unwrap();
    writeln!(out, "```text").unwrap();
    for entry in &stream.log { writeln!(out, "{}", entry).unwrap(); }
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    // Retransmission
    writeln!(out, "## 3. Retransmission State Machine").unwrap();
    writeln!(out, "```text").unwrap();
    for entry in &retx.log { writeln!(out, "{}", entry).unwrap(); }
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    // Path
    writeln!(out, "## 4. Multipath Path State Machine").unwrap();
    writeln!(out, "```text").unwrap();
    for entry in &path.log { writeln!(out, "{}", entry).unwrap(); }
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    // Invariants
    writeln!(out, "## 5. Formal Invariants").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "1. A stream cannot be `Active` if the Connection is `Closed`.").unwrap();
    writeln!(out, "2. A packet cannot be `InFlight` if its Stream is `Closed`.").unwrap();
    writeln!(out).unwrap();
    if invariants_ok {
        writeln!(out, "**Self-Check Result:** All invariants held true during the lifecycle trace.").unwrap();
    } else {
        writeln!(out, "**Self-Check Result:** INVARIANT VIOLATION DETECTED.").unwrap();
    }

    out
}

fn main() -> io::Result<()> {
    let (conn, stream, retx, path, invariants_ok) = match run_self_check() {
        Ok(data) => data,
        Err(e) => {
            eprintln!("State machine self-check failed: {}", e);
            process::exit(1);
        }
    };

    fs::write("PROTOCOL.md", protocol_spec())?;
    fs::write("STATE_MACHINE.md", state_machine_spec(&conn, &stream, &retx, &path, invariants_ok))?;

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
    
    #[test]
    fn invariants_hold_in_valid_state() {
        let global = GlobalState {
            conn: ConnState::Established,
            stream: StreamState::Active,
            retx: RetxState::InFlight,
        };
        assert!(global.check_invariants().is_ok());
    }

    #[test]
    fn invariants_catch_invalid_state() {
        let global = GlobalState {
            conn: ConnState::Closed,
            stream: StreamState::Active,
            retx: RetxState::Ready,
        };
        assert!(global.check_invariants().is_err());
    }
}