# Formal State Machines

## 1. Connection State Machine
```text
Closed --Connect--> WaitSynAck
WaitSynAck --SynAckReceived--> Established
Established --Close--> WaitCloseAck
WaitCloseAck --CloseAckReceived--> Closed
```

## 2. Stream State Machine
```text
Idle --Create--> Active
Active --SendData--> Active
Active --Close--> Closed
```

## 3. Retransmission State Machine
```text
Ready --Transmit--> InFlight
InFlight --Retransmit--> InFlight
InFlight --Ack--> Acked
```

## 4. Multipath Path State Machine
```text
Available --HighLoss--> Degraded
Degraded --GoodAck--> Available
```

## 5. Formal Invariants

1. A stream cannot be `Active` if the Connection is `Closed`.
2. A packet cannot be `InFlight` if its Stream is `Closed`.

**Self-Check Result:** All invariants held true during the lifecycle trace.
