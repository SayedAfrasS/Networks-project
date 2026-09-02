#![forbid(unsafe_code)]

mod ai;
mod congestion;
mod emulator;
mod experiment;
mod features;
mod metrics;
mod mp_report;
mod multipath;
mod packet;
mod report;
mod scheduler;
mod telemetry;

use std::collections::VecDeque;
use std::env;
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use ai::AiCongestionController;
use congestion::{CongestionController, PredictiveController, SimpleAimd};
use emulator::{NetworkEmulator, SendOutcome};
use experiment::{ExperimentLogger, ExperimentResult};
use metrics::Metrics;
use mp_report::MultipathExperimentLogger;
use multipath::MultipathManager;
use packet::{decode_packet, encode_packet, Packet, Reliability, TYPE_ACK, TYPE_DATA};
use scheduler::{PriorityScheduler, ScheduledPacket};
use telemetry::TelemetryLogger;

fn parse_percent(value: &str) -> f64 {
    let parsed = value.parse::<f64>().unwrap_or(0.0);

    if parsed.is_finite() {
        parsed.clamp(0.0, 100.0)
    } else {
        0.0
    }
}

fn parse_ms(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(0).min(10_000)
}

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

fn enqueue_demo_batch(
    scheduler: &mut PriorityScheduler,
    seq: &mut u32,
    connection_id: u32,
    stream_id: u32,
) {
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
            *seq,
            0,
            message.as_bytes(),
        );

        println!(
            "Enqueued seq={} prio={} rel={:?} payload={:?}",
            *seq,
            priority,
            reliability,
            message
        );

        scheduler.enqueue(ScheduledPacket {
            priority,
            seq: *seq,
            reliability,
            encoded: packet,
            payload_preview: message.to_string(),
        });

        *seq = (*seq).wrapping_add(1);
    }
}

struct UnackedPacket {
    item: ScheduledPacket,
    packet_len: usize,
    attempts: u32,
    max_attempts: u32,
    first_send_time: Instant,
    last_send_time: Instant,
    path_id: u8,
    timeout: Duration,
}

fn send_scheduled_packets(
    socket: &UdpSocket,
    scheduler: &mut PriorityScheduler,
    controller: &mut dyn CongestionController,
    telemetry: &mut TelemetryLogger,
    metrics: &mut Metrics,
    emulator: &mut NetworkEmulator,
    multipath: &mut MultipathManager,
) -> io::Result<()> {
    let mut pending: VecDeque<ScheduledPacket> = VecDeque::new();

    while let Some(item) = scheduler.pop_next() {
        pending.push_back(item);
    }

    let mut unacked: Vec<UnackedPacket> = Vec::new();

    let mut rx_buffer = [0u8; 65535];

    while !pending.is_empty() || !unacked.is_empty() {
        loop {
            if pending.is_empty() {
                break;
            }

            let (packet_len, reliability) = {
                let front = pending.front().unwrap();
                (front.encoded.len(), front.reliability)
            };

            if reliability != Reliability::BestEffort && !controller.can_send(packet_len) {
                break;
            }

            let item = pending.pop_front().unwrap();

            let packet_len = item.encoded.len();

            let path_id = if multipath.enabled {
                multipath.choose_path(item.reliability, item.priority)
            } else {
                0
            };

            let path_delay = if multipath.enabled {
                multipath.path_max_delay_ms(path_id)
            } else {
                emulator.max_delay_ms()
            };

            let timeout = Duration::from_millis(
                400u64.saturating_add(path_delay.saturating_mul(2)),
            );

            metrics.record_send(item.reliability, item.priority);

            let _ = telemetry.log(
                "scheduled_send",
                &format!(
                    "seq={} rel={:?} prio={} len={} path={} payload={}",
                    item.seq,
                    item.reliability,
                    item.priority,
                    packet_len,
                    path_id,
                    item.payload_preview
                ),
            );

            println!(
                "[SEND] seq={} prio={} rel={:?} path={} payload={:?}",
                item.seq,
                item.priority,
                item.reliability,
                path_id,
                item.payload_preview
            );

            if item.reliability == Reliability::BestEffort {
                let outcome = if multipath.enabled {
                    multipath.send_packet(path_id, socket, &item.encoded)?
                } else {
                    emulator.send_packet(socket, &item.encoded)?
                };

                match outcome {
                    SendOutcome::Sent => {
                        println!(
                            "Sent best-effort seq={} path={}; no ACK expected",
                            item.seq,
                            path_id
                        );
                    }
                    SendOutcome::Dropped => {
                        println!(
                            "[EMULATOR] dropped best-effort seq={} path={}",
                            item.seq,
                            path_id
                        );

                        let _ = telemetry.log(
                            "emulator_drop",
                            &format!(
                                "seq={} rel=BestEffort path={}",
                                item.seq,
                                path_id
                            ),
                        );
                    }
                }

                continue;
            }

            controller.on_packet_sent(packet_len);

            let now = Instant::now();

            let outcome = if multipath.enabled {
                multipath.send_packet(path_id, socket, &item.encoded)?
            } else {
                emulator.send_packet(socket, &item.encoded)?
            };

            if outcome == SendOutcome::Dropped {
                println!(
                    "[EMULATOR] dropped seq={} path={} on attempt 1",
                    item.seq,
                    path_id
                );

                let _ = telemetry.log(
                    "emulator_drop",
                    &format!("seq={} attempt=1 path={}", item.seq, path_id),
                );
            }

            let max_attempts: u32 = match item.reliability {
                Reliability::Important => 3,
                Reliability::Guaranteed => 8,
                Reliability::BestEffort => 1,
            };

            unacked.push(UnackedPacket {
                item,
                packet_len,
                attempts: 1,
                max_attempts,
                first_send_time: now,
                last_send_time: now,
                path_id,
                timeout,
            });
        }

        if unacked.is_empty() {
            if !pending.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
            }

            continue;
        }

        socket.set_read_timeout(Some(Duration::from_millis(50)))?;

        match socket.recv_from(&mut rx_buffer) {
            Ok((n, _addr)) => {
                if let Ok(packet) = decode_packet(&rx_buffer[..n]) {
                    if packet.ptype == TYPE_ACK {
                        let pos = unacked
                            .iter()
                            .position(|u| u.item.seq == packet.ack);

                        if let Some(pos) = pos {
                            let acked = unacked.remove(pos);

                            let elapsed = acked.first_send_time.elapsed();

                            if multipath.enabled {
                                multipath.on_ack(acked.path_id, elapsed);
                            }

                            controller.on_ack(acked.packet_len, elapsed);
                            metrics.record_ack(elapsed);

                            let _ = telemetry.log(
                                "ack",
                                &format!(
                                    "seq={} path={} rtt_us={} attempts={} {}",
                                    acked.item.seq,
                                    acked.path_id,
                                    elapsed.as_micros(),
                                    acked.attempts,
                                    controller.status()
                                ),
                            );

                            println!(
                                "ACK received seq={}, path={}, rtt={:?}, attempts={}, {}",
                                acked.item.seq,
                                acked.path_id,
                                elapsed,
                                acked.attempts,
                                controller.status()
                            );
                        }
                    }
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                // No ACK arrived during this short polling window.
            }
            Err(e) => return Err(e),
        }

        let now = Instant::now();

        let mut i = 0;

        while i < unacked.len() {
            let timed_out =
                now.duration_since(unacked[i].last_send_time) >= unacked[i].timeout;

            if !timed_out {
                i += 1;
                continue;
            }

            if unacked[i].attempts >= unacked[i].max_attempts {
                let failed = unacked.remove(i);

                if multipath.enabled {
                    multipath.on_loss(failed.path_id);
                }

                controller.on_loss(failed.packet_len);
                metrics.record_loss();

                let _ = telemetry.log(
                    "loss",
                    &format!(
                        "seq={} path={} attempts={} {}",
                        failed.item.seq,
                        failed.path_id,
                        failed.attempts,
                        controller.status()
                    ),
                );

                println!(
                    "Delivery failed seq={} path={} after {} attempts, {}",
                    failed.item.seq,
                    failed.path_id,
                    failed.attempts,
                    controller.status()
                );

                continue;
            }

            unacked[i].attempts += 1;

            let new_path_id = if multipath.enabled {
                multipath.choose_path(
                    unacked[i].item.reliability,
                    unacked[i].item.priority,
                )
            } else {
                unacked[i].path_id
            };

            let path_delay = if multipath.enabled {
                multipath.path_max_delay_ms(new_path_id)
            } else {
                emulator.max_delay_ms()
            };

            unacked[i].timeout = Duration::from_millis(
                400u64.saturating_add(path_delay.saturating_mul(2)),
            );

            unacked[i].path_id = new_path_id;

            metrics.record_retransmit();
            controller.on_retransmit();

            if multipath.enabled {
                multipath.on_retransmit(new_path_id);
            }

            println!(
                "Timeout for seq={}; retransmit attempt {}/{} path={}",
                unacked[i].item.seq,
                unacked[i].attempts,
                unacked[i].max_attempts,
                new_path_id
            );

            let _ = telemetry.log(
                "retransmit",
                &format!(
                    "seq={} path={} attempt={}/{}",
                    unacked[i].item.seq,
                    new_path_id,
                    unacked[i].attempts,
                    unacked[i].max_attempts
                ),
            );

            let outcome = if multipath.enabled {
                multipath.send_packet(new_path_id, socket, &unacked[i].item.encoded)?
            } else {
                emulator.send_packet(socket, &unacked[i].item.encoded)?
            };

            if outcome == SendOutcome::Dropped {
                println!(
                    "[EMULATOR] dropped seq={} path={} on attempt {}",
                    unacked[i].item.seq,
                    new_path_id,
                    unacked[i].attempts
                );

                let _ = telemetry.log(
                    "emulator_drop",
                    &format!(
                        "seq={} path={} attempt={}",
                        unacked[i].item.seq,
                        new_path_id,
                        unacked[i].attempts
                    ),
                );
            }

            unacked[i].last_send_time = Instant::now();

            i += 1;
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

    let mut controller: Box<dyn CongestionController> =
        Box::new(PredictiveController::new(1200));

    let mut telemetry = TelemetryLogger::new("telemetry.jsonl")?;
    let mut scheduler = PriorityScheduler::new();
    let mut metrics = Metrics::new();
    let mut emulator = NetworkEmulator::new();
    let mut multipath = MultipathManager::new();

    println!("Client ready. Sending to {}", server_address);
    println!("Congestion controller: {}", controller.name());
    println!("Controller status: {}", controller.status());
    println!("Emulator: {}", emulator.status());
    println!("Multipath: {}", multipath.status());
    println!();
    println!("Normal format: <reliability> <priority> <message>");
    println!("Reliability values: be | important | guaranteed");
    println!("Extra commands:");
    println!("  stats       Show metrics and features");
    println!("  features    Show feature extraction details");
    println!("  batch       Send demo packets");
    println!("  experiment <runs>");
    println!("              Run repeated batch experiments");
    println!("  summary     Generate summary report from experiment results");
    println!("  mpsummary   Generate multipath summary report");
    println!("  aimd        Switch to baseline AIMD controller");
    println!("  predictive  Switch to predictive controller");
    println!("  ai          Switch to simple AI controller");
    println!("  controller  Show current controller");
    println!("  reset       Reset metrics");
    println!("  loss <p>    Set loss percent");
    println!("  delay <ms>  Set delay in ms");
    println!("  jitter <ms> Set jitter in ms");
    println!("  scenario <good|lossy|bad>");
    println!("  multipath on|off|good|lossy|mixed|bad");
    println!("  emulation   Show emulator settings");
    println!("  clear       Clear emulator settings");
    println!("  help        Show help");
    println!("  exit        Quit");
    println!();

    let _ = telemetry.log(
        "client_start",
        &format!(
            "server={} controller={} {} emulator={} multipath={}",
            server_address,
            controller.name(),
            controller.status(),
            emulator.status(),
            multipath.status()
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
                println!("  stats       Show metrics and features");
                println!("  features    Show feature extraction details");
                println!("  batch       Send demo packets");
                println!("  experiment <runs>");
                println!("              Run repeated batch experiments");
                println!("  summary     Generate summary report from experiment results");
                println!("  mpsummary   Generate multipath summary report");
                println!("  aimd        Switch to baseline AIMD controller");
                println!("  predictive  Switch to predictive controller");
                println!("  ai          Switch to simple AI controller");
                println!("  controller  Show current controller");
                println!("  reset       Reset metrics");
                println!("  loss <p>    Set loss percent");
                println!("  delay <ms>  Set delay in ms");
                println!("  jitter <ms> Set jitter in ms");
                println!("  scenario <good|lossy|bad>");
                println!("  multipath on|off|good|lossy|mixed|bad");
                println!("  emulation   Show emulator settings");
                println!("  clear       Clear emulator settings");
                println!("  help        Show this help");
                println!("  exit        Quit client");
                continue;
            }
            "stats" => {
                let controller_status = controller.status();
                let features_text = controller.features_text();
                let summary = metrics.summary();
                let emulator_status = emulator.status();
                let multipath_status = multipath.status();

                println!("Controller status: {}", controller_status);
                println!("Features: {}", features_text);
                println!("Emulator: {}", emulator_status);
                println!("Multipath: {}", multipath_status);
                println!("Metrics summary: {}", summary);

                let _ = telemetry.log(
                    "stats",
                    &format!(
                        "{} {} {} {} {}",
                        controller_status,
                        features_text,
                        emulator_status,
                        multipath_status,
                        summary
                    ),
                );

                continue;
            }
            "features" => {
                let features_text = controller.features_text();

                println!("Features: {}", features_text);

                let _ = telemetry.log("features", &features_text);

                continue;
            }
            "summary" => {
                println!("Reading experiment_results.csv...");

                match report::summarize_experiment_csv(
                    "experiment_results.csv",
                    "summary_results.csv",
                ) {
                    Ok(rows) => {
                        println!("Summary written to summary_results.csv");

                        if rows.is_empty() {
                            println!("No experiment rows found.");
                        } else {
                            for row in &rows {
                                println!("{}", row.short());
                            }
                        }

                        let _ = telemetry.log(
                            "summary_generated",
                            &format!("rows={}", rows.len()),
                        );
                    }
                    Err(e) => {
                        eprintln!("Summary failed: {}", e);
                    }
                }

                continue;
            }
            "mpsummary" => {
                println!("Reading multipath_results.csv...");

                match mp_report::summarize_multipath_csv(
                    "multipath_results.csv",
                    "multipath_summary.csv",
                ) {
                    Ok(rows) => {
                        println!("Summary written to multipath_summary.csv");

                        if rows.is_empty() {
                            println!("No multipath experiment rows found.");
                        } else {
                            for row in &rows {
                                println!("{}", row.short());
                            }
                        }

                        let _ = telemetry.log(
                            "multipath_summary_generated",
                            &format!("rows={}", rows.len()),
                        );
                    }
                    Err(e) => {
                        eprintln!("Multipath summary failed: {}", e);
                    }
                }

                continue;
            }
            "controller" => {
                println!("Current controller: {}", controller.name());
                continue;
            }
            "aimd" => {
                controller = Box::new(SimpleAimd::new(1200));

                let message = format!("switched to {}", controller.name());

                println!("Switched to {}", controller.name());

                let _ = telemetry.log("controller_switch", &message);

                continue;
            }
            "predictive" => {
                controller = Box::new(PredictiveController::new(1200));

                let message = format!("switched to {}", controller.name());

                println!("Switched to {}", controller.name());

                let _ = telemetry.log("controller_switch", &message);

                continue;
            }
            "ai" => {
                controller = Box::new(AiCongestionController::new(1200));

                let message = format!("switched to {}", controller.name());

                println!("Switched to {}", controller.name());

                let _ = telemetry.log("controller_switch", &message);

                continue;
            }
            "reset" => {
                metrics = Metrics::new();

                println!("Metrics reset");

                let _ = telemetry.log("metrics_reset", "metrics reset");

                continue;
            }
            "loss" => {
                let value = line.split_whitespace().nth(1).unwrap_or("0");
                let percent = parse_percent(value);

                emulator.set_loss(percent);

                println!("Loss set: {}", emulator.status());

                let _ = telemetry.log("emulation_loss", &emulator.status());

                continue;
            }
            "delay" => {
                let value = line.split_whitespace().nth(1).unwrap_or("0");
                let ms = parse_ms(value);

                emulator.set_delay(ms);

                println!("Delay set: {}", emulator.status());

                let _ = telemetry.log("emulation_delay", &emulator.status());

                continue;
            }
            "jitter" => {
                let value = line.split_whitespace().nth(1).unwrap_or("0");
                let ms = parse_ms(value);

                emulator.set_jitter(ms);

                println!("Jitter set: {}", emulator.status());

                let _ = telemetry.log("emulation_jitter", &emulator.status());

                continue;
            }
            "scenario" => {
                let name = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_lowercase();

                match name.as_str() {
                    "good" => {
                        emulator.clear();
                    }
                    "lossy" => {
                        emulator.set_loss(20.0);
                        emulator.set_delay(20);
                        emulator.set_jitter(10);
                    }
                    "bad" => {
                        emulator.set_loss(35.0);
                        emulator.set_delay(150);
                        emulator.set_jitter(100);
                    }
                    _ => {
                        println!("Unknown scenario. Use good, lossy, or bad.");
                        continue;
                    }
                }

                println!("Scenario '{}': {}", name, emulator.status());

                let _ = telemetry.log(
                    "emulation_scenario",
                    &format!("scenario={} {}", name, emulator.status()),
                );

                continue;
            }
            "multipath" | "mp" => {
                let argument = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_lowercase();

                match argument.as_str() {
                    "" => {
                        println!("Multipath: {}", multipath.status());
                    }
                    "on" => {
                        multipath.enabled = true;

                        println!("Multipath enabled");

                        let _ = telemetry.log(
                            "multipath_enabled",
                            &multipath.status(),
                        );
                    }
                    "off" => {
                        multipath.enabled = false;

                        println!("Multipath disabled");

                        let _ = telemetry.log(
                            "multipath_disabled",
                            &multipath.status(),
                        );
                    }
                    "good" | "lossy" | "mixed" | "bad" => {
                        multipath.set_scenario(&argument);

                        println!("Multipath scenario: {}", multipath.status());

                        let _ = telemetry.log(
                            "multipath_scenario",
                            &format!("scenario={} {}", argument, multipath.status()),
                        );
                    }
                    _ => {
                        println!("Unknown multipath command.");
                        println!("Use: multipath on|off|good|lossy|mixed|bad");
                    }
                }

                continue;
            }
            "emulation" | "emu" => {
                println!("Emulator: {}", emulator.status());

                let _ = telemetry.log("emulation_status", &emulator.status());

                continue;
            }
            "clear" => {
                emulator.clear();

                println!("Emulator cleared");

                let _ = telemetry.log("emulation_clear", "cleared");

                continue;
            }
            "experiment" | "exp" => {
                let runs = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("5")
                    .parse::<u32>()
                    .unwrap_or(5)
                    .clamp(1, 100);

                let emulator_description = if multipath.enabled {
                    format!("{} multipath=on", emulator.status())
                } else {
                    emulator.status()
                };

                println!(
                    "Starting experiment: runs={} controller={} emulator={}",
                    runs,
                    controller.name(),
                    emulator_description
                );

                let mut logger = match ExperimentLogger::new("experiment_results.csv") {
                    Ok(logger) => logger,
                    Err(e) => {
                        eprintln!("Failed to open experiment_results.csv: {}", e);
                        continue;
                    }
                };

                let mut mp_logger = if multipath.enabled {
                    match MultipathExperimentLogger::new("multipath_results.csv") {
                        Ok(logger) => Some(logger),
                        Err(e) => {
                            eprintln!("Failed to open multipath_results.csv: {}", e);
                            None
                        }
                    }
                } else {
                    None
                };

                for run_id in 1..=runs {
                    metrics = Metrics::new();
                    scheduler = PriorityScheduler::new();

                    if multipath.enabled {
                        multipath.reset_stats();
                    }

                    let start = Instant::now();

                    enqueue_demo_batch(
                        &mut scheduler,
                        &mut seq,
                        connection_id,
                        stream_id,
                    );

                    if let Err(e) = send_scheduled_packets(
                        &socket,
                        &mut scheduler,
                        &mut *controller,
                        &mut telemetry,
                        &mut metrics,
                        &mut emulator,
                        &mut multipath,
                    ) {
                        eprintln!("Experiment run failed: {}", e);
                        break;
                    }

                    let duration = start.elapsed();

                    let rtt_avg_us = if metrics.rtt_samples == 0 {
                        0
                    } else {
                        metrics.rtt_sum_us / metrics.rtt_samples as u128
                    };

                    let emulator_description = if multipath.enabled {
                        format!("{} multipath=on", emulator.status())
                    } else {
                        emulator.status()
                    };

                    let result = ExperimentResult {
                        timestamp_us: experiment::timestamp_us(),
                        run_id,

                        controller: controller.name().to_string(),
                        emulator: emulator_description,

                        sent_total: metrics.sent_total,
                        sent_best_effort: metrics.sent_best_effort,
                        sent_important: metrics.sent_important,
                        sent_guaranteed: metrics.sent_guaranteed,

                        acks: metrics.acks,
                        losses: metrics.losses,
                        retransmits: metrics.retransmits,

                        rtt_samples: metrics.rtt_samples,
                        rtt_avg_us,
                        rtt_min_us: metrics.rtt_min_us.unwrap_or(0),
                        rtt_max_us: metrics.rtt_max_us.unwrap_or(0),

                        final_cwnd_bytes: controller.cwnd_bytes(),
                        final_risk: controller.risk(),

                        duration_ms: duration.as_millis(),
                    };

                    if let Err(e) = logger.write_result(&result) {
                        eprintln!("Failed to write experiment result: {}", e);
                        break;
                    }

                    if multipath.enabled {
                        if let Some(mp_logger) = mp_logger.as_mut() {
                            if let Err(e) = mp_logger.write_run(
                                run_id,
                                controller.name(),
                                &multipath,
                            ) {
                                eprintln!("Failed to write multipath result: {}", e);
                                break;
                            }
                        }
                    }

                    println!(
                        "[EXPERIMENT] run {}/{} done: {}",
                        run_id,
                        runs,
                        result.summary_short()
                    );

                    std::thread::sleep(Duration::from_millis(100));
                }

                println!("Experiment complete. Results saved to experiment_results.csv");

                if multipath.enabled {
                    println!("Multipath results saved to multipath_results.csv");
                }

                continue;
            }
            "batch" | "demo" => {
                enqueue_demo_batch(
                    &mut scheduler,
                    &mut seq,
                    connection_id,
                    stream_id,
                );

                let _ = telemetry.log(
                    "batch_enqueued",
                    &format!("count={}", scheduler.len()),
                );

                send_scheduled_packets(
                    &socket,
                    &mut scheduler,
                    &mut *controller,
                    &mut telemetry,
                    &mut metrics,
                    &mut emulator,
                    &mut multipath,
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
            &mut *controller,
            &mut telemetry,
            &mut metrics,
            &mut emulator,
            &mut multipath,
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