#![forbid(unsafe_code)]

mod congestion;
mod metrics;
mod packet;
mod scheduler;
mod telemetry;

use std::env;
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use congestion::{CongestionController, SimpleAimd};
use metrics::Metrics;
use packet::{decode_packet, encode_packet, Packet, Reliability, TYPE_ACK, TYPE_DATA};
use scheduler::{PriorityScheduler, ScheduledPacket};
use telemetry::TelemetryLogger;

fn send_ack(socket: &UdpSocket, destination: SocketAddr, packet: &Packet) -> io::Result<usize> {
    let ack_packet = encode_packet(
        TYPE_ACK,
        Reliability::BestEffort,
        0,
        packet.connection_id,
        packet.stream_id,
        0,
        packet.seq,
        &[],
    );

    socket.send_to(&ack_packet, destination)
}

fn wait_for_ack(socket: &UdpSocket, expected_seq: u32) -> io::Result<bool> {
    let mut buffer = [0u8; 65535];

    let timeout = Duration::from_millis(400);
    let deadline = Instant::now() + timeout;

    loop {
        let now = Instant::now();

        if now >= deadline {
            return Ok(false);
        }

        let remaining = deadline.saturating_duration_since(now);
        socket.set_read_timeout(Some(remaining))?;

        match socket.recv_from(&mut buffer) {
            Ok((n, _addr)) => {
                if let Ok(packet) = decode_packet(&buffer[..n]) {
                    if packet.ptype == TYPE_ACK && packet.ack == expected_seq {
                        return Ok(true);
                    }
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                return Ok(false);
            }
            Err(e) => return Err(e),
        }
    }
}

fn send_scheduled_packets(
    socket: &UdpSocket,
    scheduler: &mut PriorityScheduler,
    controller: &mut dyn CongestionController,
    telemetry: &mut TelemetryLogger,
    metrics: &mut Metrics,
) -> io::Result<()> {
    while let Some(item) = scheduler.pop_next() {
        let packet_len = item.encoded.len();

        metrics.record_send(item.reliability, item.priority);

        let _ = telemetry.log(
            "scheduled_send",
            &format!(
                "seq={} rel={:?} prio={} len={} payload={}",
                item.seq,
                item.reliability,
                item.priority,
                packet_len,
                item.payload_preview
            ),
        );

        println!(
            "[SEND] seq={} prio={} rel={:?} payload={:?}",
            item.seq,
            item.priority,
            item.reliability,
            item.payload_preview
        );

        if item.reliability == Reliability::BestEffort {
            socket.send(&item.encoded)?;

            println!(
                "Sent best-effort seq={}; no ACK expected",
                item.seq
            );

            continue;
        }

        while !controller.can_send(packet_len) {
            println!("Waiting for congestion window: {}", controller.status());
            std::thread::sleep(Duration::from_millis(20));
        }

        controller.on_packet_sent(packet_len);

        let start_time = Instant::now();

        socket.send(&item.encoded)?;

        let max_attempts: u32 = match item.reliability {
            Reliability::Important => 3,
            Reliability::Guaranteed => 8,
            Reliability::BestEffort => 1,
        };

        let mut attempt = 1;
        let mut acked = false;

        while attempt <= max_attempts {
            match wait_for_ack(socket, item.seq) {
                Ok(true) => {
                    acked = true;
                    break;
                }
                Ok(false) => {
                    if attempt < max_attempts {
                        println!(
                            "Timeout for seq={}; retransmit attempt {}/{}",
                            item.seq,
                            attempt + 1,
                            max_attempts
                        );

                        metrics.record_retransmit();

                        let _ = telemetry.log(
                            "retransmit",
                            &format!(
                                "seq={} attempt={}/{}",
                                item.seq,
                                attempt + 1,
                                max_attempts
                            ),
                        );

                        socket.send(&item.encoded)?;
                    }
                }
                Err(e) => {
                    eprintln!("Receive error: {}", e);
                    break;
                }
            }

            attempt += 1;
        }

        let elapsed = start_time.elapsed();

        if acked {
            controller.on_ack(packet_len, elapsed);
            metrics.record_ack(elapsed);

            let _ = telemetry.log(
                "ack",
                &format!(
                    "seq={} rtt_us={} attempts={} {}",
                    item.seq,
                    elapsed.as_micros(),
                    attempt,
                    controller.status()
                ),
            );

            println!(
                "ACK received seq={}, rtt={:?}, attempts={}, {}",
                item.seq,
                elapsed,
                attempt,
                controller.status()
            );
        } else {
            controller.on_loss(packet_len);
            metrics.record_loss();

            let _ = telemetry.log(
                "loss",
                &format!(
                    "seq={} attempts={} {}",
                    item.seq,
                    max_attempts,
                    controller.status()
                ),
            );

            println!(
                "Delivery failed seq={} after {} attempts, {}",
                item.seq,
                max_attempts,
                controller.status()
            );
        }
    }

    Ok(())
}

fn run_server(bind_address: &str) -> io::Result<()> {
    let socket = UdpSocket::bind(bind_address)?;

    println!("Server listening on {}", bind_address);

    let mut buffer = [0u8; 65535];

    loop {
        let (n, source) = socket.recv_from(&mut buffer)?;

        match decode_packet(&buffer[..n]) {
            Ok(packet) => {
                match packet.ptype {
                    TYPE_DATA => {
                        let text = String::from_utf8_lossy(&packet.payload);

                        println!(
                            "[DATA] v={} conn={} stream={} seq={} rel={:?} prio={} ts={} payload={:?}",
                            packet.version,
                            packet.connection_id,
                            packet.stream_id,
                            packet.seq,
                            packet.reliability,
                            packet.priority,
                            packet.timestamp_us,
                            text
                        );

                        if packet.reliability != Reliability::BestEffort {
                            send_ack(&socket, source, &packet)?;
                        }
                    }
                    TYPE_ACK => {
                        println!(
                            "[ACK] conn={} stream={} ack={}",
                            packet.connection_id,
                            packet.stream_id,
                            packet.ack
                        );
                    }
                    _ => {
                        println!("[UNKNOWN] type={}", packet.ptype);
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid packet from {}: {}", source, e);
            }
        }
    }
}

fn run_client(server_address: &str) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(server_address)?;
    socket.set_read_timeout(Some(Duration::from_millis(400)))?;

    let mut controller = SimpleAimd::new(1200);
    let mut telemetry = TelemetryLogger::new("telemetry.jsonl")?;
    let mut scheduler = PriorityScheduler::new();
    let mut metrics = Metrics::new();

    println!("Client ready. Sending to {}", server_address);
    println!("Congestion controller: {}", controller.name());
    println!("Controller status: {}", controller.status());
    println!();
    println!("Normal format: <reliability> <priority> <message>");
    println!("Reliability values: be | important | guaranteed");
    println!("Extra commands: stats | batch | help | exit");
    println!();

    let _ = telemetry.log(
        "client_start",
        &format!(
            "server={} controller={} {}",
            server_address,
            controller.name(),
            controller.status()
        ),
    );

    let stdin = io::stdin();

    let mut seq: u32 = 1;

    let connection_id: u32 = 0x00AB_1234;
    let stream_id: u32 = 1;

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut line = String::new();

        if stdin.lock().read_line(&mut line)? == 0 {
            break;
        }

        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        let command = line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_lowercase();

        match command.as_str() {
            "quit" | "exit" => {
                break;
            }
            "help" => {
                println!("Normal format: <reliability> <priority> <message>");
                println!("Reliability values: be | important | guaranteed");
                println!();
                println!("Commands:");
                println!("  stats   Show metrics summary");
                println!("  batch   Send demo packets using priority scheduler");
                println!("  help    Show this help");
                println!("  exit    Quit client");
                continue;
            }
            "stats" => {
                let controller_status = controller.status();
                let summary = metrics.summary();

                println!("Controller status: {}", controller_status);
                println!("Metrics summary: {}", summary);

                let _ = telemetry.log(
                    "stats",
                    &format!("{} {}", controller_status, summary),
                );

                continue;
            }
            "batch" | "demo" => {
                println!("Enqueuing demo packets...");

                let demo_packets = vec![
                    (Reliability::BestEffort, 0, "demo best effort"),
                    (Reliability::Important, 2, "demo sensor"),
                    (Reliability::Important, 6, "demo alert"),
                    (Reliability::Guaranteed, 4, "demo file chunk"),
                    (Reliability::Guaranteed, 7, "demo critical control"),
                ];

                for (reliability, priority, message) in demo_packets {
                    let packet = encode_packet(
                        TYPE_DATA,
                        reliability,
                        priority,
                        connection_id,
                        stream_id,
                        seq,
                        0,
                        message.as_bytes(),
                    );

                    println!(
                        "Enqueued seq={} prio={} rel={:?} payload={:?}",
                        seq, priority, reliability, message
                    );

                    scheduler.enqueue(ScheduledPacket {
                        priority,
                        seq,
                        reliability,
                        encoded: packet,
                        payload_preview: message.to_string(),
                    });

                    seq = seq.wrapping_add(1);
                }

                let _ = telemetry.log(
                    "batch_enqueued",
                    &format!("count={}", scheduler.len()),
                );

                send_scheduled_packets(
                    &socket,
                    &mut scheduler,
                    &mut controller,
                    &mut telemetry,
                    &mut metrics,
                )?;

                continue;
            }
            _ => {}
        }

        let mut parts = line.split_whitespace();

        let reliability_str = parts.next().unwrap_or_default();
        let priority_str = parts.next().unwrap_or_default();

        let message: String = parts.collect::<Vec<_>>().join(" ");

        if reliability_str.is_empty() || message.is_empty() {
            println!("Usage: <reliability> <priority> <message>");
            println!("Type help for more commands.");
            continue;
        }

        let reliability = match reliability_str.to_lowercase().as_str() {
            "be" | "best" | "besteffort" => Reliability::BestEffort,
            "important" | "imp" => Reliability::Important,
            "guaranteed" | "reliable" | "rel" => Reliability::Guaranteed,
            _ => {
                println!(
                    "Unknown reliability or command '{}'. Type help.",
                    reliability_str
                );
                continue;
            }
        };

        let priority = priority_str.parse::<u8>().unwrap_or(0);

        let payload = message.clone().into_bytes();

        let packet = encode_packet(
            TYPE_DATA,
            reliability,
            priority,
            connection_id,
            stream_id,
            seq,
            0,
            &payload,
        );

        scheduler.enqueue(ScheduledPacket {
            priority,
            seq,
            reliability,
            encoded: packet,
            payload_preview: message,
        });

        seq = seq.wrapping_add(1);

        send_scheduled_packets(
            &socket,
            &mut scheduler,
            &mut controller,
            &mut telemetry,
            &mut metrics,
        )?;
    }

    let _ = telemetry.log("client_stop", "client stopped");

    Ok(())
}

fn print_usage() {
    println!("Adaptive Transport Prototype");
    println!();
    println!("Usage:");
    println!("  cargo run -- server <bind-address>");
    println!("  cargo run -- client <server-address>");
    println!();
    println!("Examples:");
    println!("  cargo run -- server 127.0.0.1:9000");
    println!("  cargo run -- client 127.0.0.1:9000");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        print_usage();
        return;
    }

    let mode = args[1].to_lowercase();
    let target = args[2].as_str();

    let result = match mode.as_str() {
        "server" => run_server(target),
        "client" => run_client(target),
        _ => {
            print_usage();
            return;
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}