# Connection State Machine

## States

- Closed
- WaitSynAck
- Established
- WaitCloseAck

## Events

- Connect
- SynAckReceived
- Close
- CloseAckReceived
- Timeout

## Valid Transitions

```text
Closed --Connect--> WaitSynAck
WaitSynAck --SynAckReceived--> Established
WaitSynAck --Timeout--> Closed
Established --Close--> WaitCloseAck
WaitCloseAck --CloseAckReceived--> Closed
WaitCloseAck --Timeout--> Closed
```

## Self-Check Trace

```text
Closed --Connect--> WaitSynAck
WaitSynAck --SynAckReceived--> Established
Established --Close--> WaitCloseAck
WaitCloseAck --CloseAckReceived--> Closed
```

## Stream State

Each stream is currently modeled with simple states:

- created
- active
- closed when connection closes

## Retransmission State

Each reliable packet has:

- first send time
- last send time
- attempt count
- maximum attempts
- path ID
- stream ID

