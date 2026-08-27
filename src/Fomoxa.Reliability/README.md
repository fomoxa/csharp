# Fomoxa.Reliability

## 1. Overview

Fomoxa.Reliability adds a per-send choice between reliable and unreliable delivery on top of one Fomoxa session, for an application that would otherwise run entirely on a Message-kind (unreliable) transport such as UDP.

It changes nothing in `Fomoxa.Net`. `ReliableChannel` wraps one `FomoxaConnection`, and `ReliableServer` wraps one `FomoxaServer`; both hold the wrapped object internally and drive it through its ordinary public API. No `ITransport` implementation, and no method on `FomoxaConnection` or `FomoxaServer`, is touched.

## 2. What this adds

Fomoxa's own wire format and core give every message on a session the same delivery guarantee: whatever the underlying transport's `TransportKind` provides, and nothing else. There is no per-message reliability flag, no resend, and no duplicate filtering - by design, per the Fomoxa implementation guide.

This library adds exactly that, entirely at the application layer, using two reserved message types:

```
SendUnreliable(id, payload)   ->  connection.Send(id, payload)               // unchanged, straight through
SendReliable(id, payload)     ->  connection.Send(EnvelopeId, [Seq][id][len][payload])
                                   resent until the peer's Ack arrives
```

The receiving side unwraps the envelope before the application ever sees it: the event you get back carries the original message id and payload, with an added `WasReliable` flag. A retransmit that arrives after its own Ack raced past it is deduplicated by `Seq` before delivery, so the application never sees the same reliable message twice.

`ReliableEvent.Payload` does not outlive the call that produced it. It follows the same rule as `FomoxaEvent.Payload` in Fomoxa.Net: it references a buffer this library reuses on the next `Tick` call, reliable or not. Read it (or copy it with `CopyPayload()`) before that next `Tick` - holding onto `raised.Payload` across frames, which is an easy habit to fall into in a Unity `Update`/coroutine, will read back corrupted or unrelated data once the buffer has been overwritten.

## 3. Installation

```sh
dotnet add package Fomoxa.Reliability
```

The package references `Fomoxa.Net` and declares no other dependency that reaches a consumer.

## 4. Quick Start

### 4.1 Client

```csharp
using Fomoxa.Net;
using Fomoxa.Net.Transports;
using Fomoxa.Reliability;

var transport = UdpTransport.Connect(new IPEndPoint(IPAddress.Loopback, 7777));
using var channel = ReliableChannel.Connect(transport, MySchema.Build(), new SessionConfig());

while (!channel.IsClosed)
{
    foreach (var raised in channel.Tick())
    {
        switch (raised.Kind)
        {
            case ReliableEventKind.Message:
                Handle(raised.MessageId, raised.Payload.Span);
                break;

            case ReliableEventKind.DeliveryFailed:
                Log($"message {raised.MessageId} never got an Ack");
                break;
        }
    }

    if (channel.IsReady)
    {
        channel.SendUnreliable(MySchema.SteeringMessageId, steeringPayload);
        channel.SendReliable(MySchema.RoleAssignedMessageId, rolePayload);
    }

    Thread.Sleep(16);
}
```

### 4.2 Server

```csharp
using var server = new ReliableServer(
    new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 7777)),
    MySchema.Build(),
    new SessionConfig());

while (true)
{
    foreach (var raised in server.Tick())
    {
        if (raised.Kind == ReliableEventKind.Message)
        {
            Handle(raised.PeerId, raised.MessageId, raised.Payload.Span);
        }
    }

    server.BroadcastUnreliable(MySchema.SnapshotMessageId, snapshotPayload);
    Thread.Sleep(16);
}
```

## 5. Configuration

`ReliabilityConfig`, passed to `ReliableChannel`/`ReliableServer` alongside the usual `SessionConfig`:

| Setting | Default | Meaning |
|---|---|---|
| `ResendInterval` | 200 ms | Silence before a reliable send still waiting on an Ack is resent |
| `MaxAttempts` | 10 | Real transmissions of one reliable send before it is reported `DeliveryFailed` and dropped. A send that only got `Congested`/`NotReady` locally does not spend an attempt |
| `DedupeWindowSize` | 256 | How many recent inbound `Seq` values are remembered, so a very-late duplicate past this window is treated as new again |
| `MaxPendingSends` | 256 | Ceiling on reliable sends awaiting an Ack at once; `SendReliable` returns `SendStatus.Congested` once reached, instead of growing the pending table without a bound |
| `MaxAcksPerMessage` | 16 | Ceiling on how many times this side re-acks the same inbound `Seq`; keep it above the peer's `MaxAttempts` or a legitimate retry may stop getting replies |

`ResendInterval` is a fixed wait, not an RTT estimate - it does not adapt to the measured round-trip time the way TCP's retransmission timeout does. Set it comfortably above your expected round-trip time: too low wastes bandwidth resending before an Ack could realistically have come back, too high delays recovery from a real lost packet. The 200 ms default targets a LAN or another low, stable-latency link; raise it for a connection with higher or more variable latency.

## 6. Observability

`ReliableChannel.Metrics` and `ReliableServer.PeerMetrics(peerId)`/`AggregateMetrics()` return a `ReliabilityMetrics` snapshot - plain counters already maintained on the hot path, so reading one costs nothing beyond copying it. Meant to be polled occasionally (a debug overlay once a second, a log line when a peer disconnects), not every tick.

```csharp
var m = channel.Metrics;
Log($"pending={m.PendingCount} resent={m.ReliableResent} failed={m.DeliveryFailed}");
```

| Field | What a nonzero value means |
|---|---|
| `PendingCount` | Reliable sends awaiting an Ack right now - the only non-cumulative field; a rising trend is the first sign of a peer not keeping up |
| `ReliableSent` | `SendReliable` calls accepted and tracked |
| `ReliableResent` | Retransmissions beyond each message's first attempt - high relative to `ReliableSent` means a lossy link, or `MaxAttempts`/`ResendInterval` tuned too aggressively for it |
| `Delivered` | Reliable sends confirmed by an Ack |
| `DeliveryFailed` | Reliable sends that exhausted `MaxAttempts` - should stay at zero; nonzero means real, lasting loss |
| `RejectedPendingFull` | `SendReliable` calls refused because `MaxPendingSends` was already reached |
| `EnvelopesReceived` | Inbound envelope arrivals, new or duplicate |
| `DuplicatesDropped` | Inbound arrivals not delivered again - expected under loss, it means resend is doing its job |
| `AcksSent` | Ack frames actually sent |
| `AcksSuppressed` | Ack replies skipped because `MaxAcksPerMessage` was already spent for that `Seq` |

A peer that has disconnected contributes nothing to `AggregateMetrics()` and has no entry for `PeerMetrics` - its counters leave with it, same as the rest of its reliability state (§5's `MaxPendingSends`/`MaxAcksPerMessage` bookkeeping).

## 7. What this does not do

- No ordering guarantee across different reliable messages. Each `SendReliable` call is acked and retried independently; two reliable sends made close together can still be delivered out of order if only one of them is lost and has to be retried. If two reliable messages have a real ordering dependency, that dependency must be enforced by the application (for example, by not sending the second until the first's `Delivered` event arrives).
- Not a replacement for choosing a reliable transport. An application that needs strict in-order, no-loss delivery for most of its traffic is better served by a Stream-kind transport (TCP, or a reliable QUIC stream) and plain `FomoxaConnection`/`FomoxaServer`. This library targets the opposite case: mostly-unreliable traffic with a small, rare subset of messages that must not be lost.
- Both peers must use this library, not one bare `FomoxaConnection`/`FomoxaServer` and one wrapped. The handshake still succeeds either way - the two reserved message ids simply go unmatched, per RFC-0002 §9.1 - but a peer without this library will treat an inbound envelope as an ordinary, unrecognized message rather than unwrapping it, and will never emit the `Ack` a wrapped sender is waiting for.

## 8. Building From Source

```sh
dotnet build ../../Fomoxa.Net.sln
dotnet run --project ../../tests/Fomoxa.Reliability.Tests
```

## 9. License

Apache-2.0.
