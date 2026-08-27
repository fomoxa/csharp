using System;
using System.Collections.Generic;
using Fomoxa.Net;

namespace Fomoxa.Reliability.Tests
{
    public static class ReliabilityTests
    {
        private static Schema BuildAppSchema() =>
            new Schema(
                0x1111_1111_2222_2222UL,
                new[] { new MessageSchema(1, 0xAAAAUL, new ulong[] { 0xAAAAUL }) });

        private static (ReliableChannel client, ReliableChannel server, FakeTransport clientWire,
            FakeTransport serverWire, TimeSpan now) MakeUnreadyPair(ReliabilityConfig? config = null)
        {
            var schema = ReliabilitySchema.Combine(BuildAppSchema());
            var sessionConfig = new SessionConfig();

            var clientWire = new FakeTransport();
            var serverWire = new FakeTransport();

            var now = TimeSpan.Zero;
            var clientConnection = new FomoxaConnection(clientWire, schema, sessionConfig, FomoxaRole.Client, now);
            var serverConnection = new FomoxaConnection(serverWire, schema, sessionConfig, FomoxaRole.Server, now);

            var client = new ReliableChannel(clientConnection, config);
            var server = new ReliableChannel(serverConnection, config);

            return (client, server, clientWire, serverWire, now);
        }

        private static (ReliableChannel client, ReliableChannel server, FakeTransport clientWire,
            FakeTransport serverWire, TimeSpan now) MakeReadyPair(ReliabilityConfig? config = null)
        {
            var (client, server, clientWire, serverWire, now) = MakeUnreadyPair(config);

            Pipe.Exchange(clientWire, serverWire);
            for (int step = 0; step < 20 && !(client.IsReady && server.IsReady); step++)
            {
                now += TimeSpan.FromMilliseconds(16);
                client.Tick(now);
                server.Tick(now);
                Pipe.Exchange(clientWire, serverWire);
            }

            Check.True(client.IsReady, "client should reach Ready");
            Check.True(server.IsReady, "server should reach Ready");

            return (client, server, clientWire, serverWire, now);
        }

        private sealed class ServerClient
        {
            public ReliableChannel Channel = null!;
            public FakeTransport ClientWire = null!;
            public FakeTransport ServerWire = null!;
        }

        private static ServerClient AddClient(FakeListener listener, ReliabilityConfig? config, TimeSpan now)
        {
            var schema = ReliabilitySchema.Combine(BuildAppSchema());
            var clientWire = new FakeTransport();
            var serverWire = new FakeTransport();
            listener.Waiting.Enqueue(serverWire);

            var connection = new FomoxaConnection(clientWire, schema, new SessionConfig(), FomoxaRole.Client, now);
            return new ServerClient
            {
                Channel = new ReliableChannel(connection, config),
                ClientWire = clientWire,
                ServerWire = serverWire,
            };
        }

        private static (ReliableServer server, ServerClient[] clients, TimeSpan now) MakeServerWithReadyClients(
            int clientCount, ReliabilityConfig? config = null)
        {
            var listener = new FakeListener();
            var server = new ReliableServer(listener, BuildAppSchema(), new SessionConfig(), config);

            var now = TimeSpan.Zero;
            var clients = new ServerClient[clientCount];
            for (int index = 0; index < clientCount; index++)
            {
                clients[index] = AddClient(listener, config, now);
                Pipe.Exchange(clients[index].ClientWire, clients[index].ServerWire);
            }

            for (int step = 0; step < 20; step++)
            {
                bool allReady = true;
                for (int index = 0; index < clients.Length; index++)
                {
                    if (!clients[index].Channel.IsReady)
                    {
                        allReady = false;
                    }
                }

                if (allReady)
                {
                    break;
                }

                now += TimeSpan.FromMilliseconds(16);
                for (int index = 0; index < clients.Length; index++)
                {
                    clients[index].Channel.Tick(now);
                }

                server.Tick(now);

                for (int index = 0; index < clients.Length; index++)
                {
                    Pipe.Exchange(clients[index].ClientWire, clients[index].ServerWire);
                }
            }

            for (int index = 0; index < clients.Length; index++)
            {
                Check.True(clients[index].Channel.IsReady, $"client {index} should reach Ready");
            }

            return (server, clients, now);
        }

        private static ReliableEvent? FindMessage(IReadOnlyList<ReliableEvent> events, uint messageId)
        {
            foreach (var item in events)
            {
                if (item.Kind == ReliableEventKind.Message && item.MessageId == messageId)
                {
                    return item;
                }
            }

            return null;
        }

        private static bool Has(IReadOnlyList<ReliableEvent> events, ReliableEventKind kind, uint messageId)
        {
            foreach (var item in events)
            {
                if (item.Kind == kind && item.MessageId == messageId)
                {
                    return true;
                }
            }

            return false;
        }

        public static void UnreliableDeliversWithoutAck()
        {
            var (client, server, clientWire, serverWire, now) = MakeReadyPair();

            var status = client.SendUnreliable(1, new byte[] { 1, 2, 3 });
            Check.Equal(SendStatus.Sent, status, "unreliable send should be accepted");

            Pipe.Exchange(clientWire, serverWire);
            var serverEvents = server.Tick(now);

            var message = FindMessage(serverEvents, 1);
            Check.True(message.HasValue, "server should receive the unreliable message");
            Check.True(!message!.Value.WasReliable, "message should be marked unreliable");
            Check.True(serverWire.Outgoing.Count == 0, "an unreliable message must not trigger an Ack");
        }

        public static void ReliableSurvivesOneDroppedEnvelope()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(50),
                MaxAttempts = 5,
            };
            var (client, server, clientWire, serverWire, now) = MakeReadyPair(config);

            int envelopeSends = 0;
            clientWire.DropOutgoing = frame =>
            {
                if (!FrameSniff.TryMessageId(frame, out var id) || id != ReliabilitySchema.EnvelopeMessageId)
                {
                    return false;
                }

                envelopeSends++;
                return envelopeSends == 1;
            };

            var status = client.SendReliable(2, new byte[] { 9, 9 });
            Check.Equal(SendStatus.Sent, status, "reliable send should be accepted locally");

            Pipe.Exchange(clientWire, serverWire);
            var firstPass = server.Tick(now);
            Check.True(!FindMessage(firstPass, 2).HasValue, "the dropped envelope must not reach the app yet");

            now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
            client.Tick(now);
            Pipe.Exchange(clientWire, serverWire);
            var secondPass = server.Tick(now);

            var delivered = FindMessage(secondPass, 2);
            Check.True(delivered.HasValue, "the resent envelope should reach the app");
            Check.True(delivered!.Value.WasReliable, "message should be marked reliable");

            Pipe.Exchange(clientWire, serverWire);
            var clientEvents = client.Tick(now);
            Check.True(Has(clientEvents, ReliableEventKind.Delivered, 2), "sender should see Delivered once the Ack arrives");
        }

        public static void DuplicateEnvelopeIsNotDeliveredTwice()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(50),
                MaxAttempts = 5,
            };
            var (client, server, clientWire, serverWire, now) = MakeReadyPair(config);

            serverWire.DropOutgoing = frame =>
                FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.AckMessageId;

            var status = client.SendReliable(3, new byte[] { 7 });
            Check.Equal(SendStatus.Sent, status, "reliable send should be accepted locally");

            Pipe.Exchange(clientWire, serverWire);
            var firstPass = server.Tick(now);
            Check.True(FindMessage(firstPass, 3).HasValue, "first arrival should reach the app");

            serverWire.DropOutgoing = null;
            now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
            client.Tick(now);
            Pipe.Exchange(clientWire, serverWire);
            var secondPass = server.Tick(now);

            Check.True(!FindMessage(secondPass, 3).HasValue, "a resend caused by a lost Ack must not be delivered twice");
        }

        public static void CongestedReliableSendIsNotLost()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(30),
                MaxAttempts = 10,
            };
            var (client, server, clientWire, serverWire, now) = MakeReadyPair(config);

            // Simulate a slow link: the first send is accepted locally but never
            // reaches the wire this tick, so Fomoxa's own single-frame outbox is
            // occupied when the reliable send is attempted right after it.
            clientWire.BlockNextSends = 1;
            var firstStatus = client.SendUnreliable(1, new byte[] { 0 });
            Check.Equal(SendStatus.Sent, firstStatus, "the first send is accepted, just not on the wire yet");

            var congestedStatus = client.SendReliable(2, new byte[] { 9, 9 });
            Check.Equal(SendStatus.Congested, congestedStatus, "a send while the outbox is occupied must be congested");

            ReliableEvent? delivered = null;
            for (int step = 0; step < 10 && !delivered.HasValue; step++)
            {
                now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
                client.Tick(now);
                Pipe.Exchange(clientWire, serverWire);
                delivered = FindMessage(server.Tick(now), 2);
                Pipe.Exchange(clientWire, serverWire);
                client.Tick(now);
            }

            Check.True(delivered.HasValue, "a reliable send that only got Congested locally must still be delivered, not silently lost");
        }

        public static void PendingSendsAreCapped()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromSeconds(10),
                MaxAttempts = 100,
                MaxPendingSends = 2,
            };
            var (client, _, clientWire, _, _) = MakeReadyPair(config);

            clientWire.DropOutgoing = frame =>
                FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.EnvelopeMessageId;

            Check.Equal(SendStatus.Sent, client.SendReliable(10, new byte[] { 1 }), "1st reliable send should be accepted");
            Check.Equal(SendStatus.Sent, client.SendReliable(11, new byte[] { 2 }), "2nd reliable send should be accepted");
            Check.Equal(SendStatus.Congested, client.SendReliable(12, new byte[] { 3 }), "3rd reliable send should be refused once the pending table is full");
        }

        public static void AckRepliesAreCappedPerMessage()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromSeconds(10),
                MaxAttempts = 100,
                MaxAcksPerMessage = 2,
            };
            var (client, server, clientWire, serverWire, now) = MakeReadyPair(config);

            var status = client.SendReliable(5, new byte[] { 1 });
            Check.Equal(SendStatus.Sent, status, "reliable send should be accepted");
            Check.True(clientWire.Outgoing.Count == 1, "exactly one envelope frame should be queued");

            var envelopeFrame = clientWire.Outgoing.Peek();

            // Redeliver the exact same envelope frame far more times than
            // MaxAcksPerMessage allows, simulating a peer that keeps resending
            // long after a real lost-Ack retry would need.
            for (int i = 0; i < 5; i++)
            {
                serverWire.Incoming.Enqueue((byte[])envelopeFrame.Clone());
                server.Tick(now);
            }

            int ackCount = 0;
            while (serverWire.Outgoing.Count > 0)
            {
                var frame = serverWire.Outgoing.Dequeue();
                if (FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.AckMessageId)
                {
                    ackCount++;
                }
            }

            Check.Equal(config.MaxAcksPerMessage, ackCount, "acks for a repeatedly-duplicated message must stop at the configured cap");
        }

        public static void NotReadySendIsRegisteredAndEventuallyDelivered()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(30),
                MaxAttempts = 20,
            };
            var (client, server, clientWire, serverWire, now) = MakeUnreadyPair(config);

            var status = client.SendReliable(6, new byte[] { 4, 2 });
            Check.Equal(SendStatus.NotReady, status, "sending reliable before the handshake completes should be refused as NotReady");

            Pipe.Exchange(clientWire, serverWire);
            ReliableEvent? delivered = null;
            for (int step = 0; step < 30 && !delivered.HasValue; step++)
            {
                now += TimeSpan.FromMilliseconds(16);
                client.Tick(now);
                delivered = FindMessage(server.Tick(now), 6);
                Pipe.Exchange(clientWire, serverWire);
            }

            Check.True(delivered.HasValue, "a reliable send made before Ready must still be delivered once the session becomes Ready");
        }

        public static void BroadcastReliableTracksEachPeerIndependently()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(50),
                MaxAttempts = 5,
            };
            var (server, clients, now) = MakeServerWithReadyClients(2, config);
            var peerIds = server.PeerIds();
            Check.Equal(2, peerIds.Count, "both clients should be connected");

            int dropCount = 0;
            clients[0].ServerWire.DropOutgoing = frame =>
            {
                if (!FrameSniff.TryMessageId(frame, out var id) || id != ReliabilitySchema.EnvelopeMessageId)
                {
                    return false;
                }

                dropCount++;
                return dropCount == 1;
            };

            server.BroadcastReliable(20, new byte[] { 5, 5 });

            Pipe.Exchange(clients[0].ClientWire, clients[0].ServerWire);
            Pipe.Exchange(clients[1].ClientWire, clients[1].ServerWire);
            var firstPassPeer0 = clients[0].Channel.Tick(now);
            var firstPassPeer1 = clients[1].Channel.Tick(now);

            Check.True(!Has(firstPassPeer0, ReliableEventKind.Message, 20), "peer 0's broadcast was dropped, it should not have arrived yet");
            Check.True(Has(firstPassPeer1, ReliableEventKind.Message, 20), "peer 1's broadcast should have arrived immediately");

            now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
            server.Tick(now);
            Pipe.Exchange(clients[0].ClientWire, clients[0].ServerWire);
            var resent = clients[0].Channel.Tick(now);

            Check.True(Has(resent, ReliableEventKind.Message, 20), "peer 0 should receive the broadcast once resent, independent of peer 1's own delivery");
        }

        public static void DisconnectRemovesServerSidePeerState()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(20),
                MaxAttempts = 3,
            };
            var (server, clients, now) = MakeServerWithReadyClients(1, config);
            var peerId = server.PeerIds()[0];

            clients[0].ServerWire.DropOutgoing = frame =>
                FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.EnvelopeMessageId;

            var status = server.SendReliable(peerId, 30, new byte[] { 1 });
            Check.Equal(SendStatus.Sent, status, "reliable send to the peer should be accepted");

            // Simulate the peer's link dying, the way a real socket would report
            // it - Fomoxa has no close frame of its own, so this is the only
            // realistic way a disconnect becomes observable (01-overview.md §9).
            clients[0].ServerWire.ReportClosed = true;

            now += TimeSpan.FromMilliseconds(16);
            var serverEvents = server.Tick(now);
            Check.True(Has(serverEvents, ReliableEventKind.Disconnected, 0), "server should observe the peer disconnecting");
            Check.Equal(0, server.TrackedPeerCount, "no per-peer reliability state should remain right after the disconnect");

            var collected = new List<ReliableEvent>();
            for (int step = 0; step < 10; step++)
            {
                now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
                collected.AddRange(server.Tick(now));
            }

            Check.True(!Has(collected, ReliableEventKind.DeliveryFailed, 30), "a message to an already-disconnected peer must not linger and later report DeliveryFailed");
        }

        public static void MetricsTrackSendResendAndDeliver()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(30),
                MaxAttempts = 5,
            };
            var (client, server, clientWire, serverWire, now) = MakeReadyPair(config);

            int envelopeSends = 0;
            clientWire.DropOutgoing = frame =>
            {
                if (!FrameSniff.TryMessageId(frame, out var id) || id != ReliabilitySchema.EnvelopeMessageId)
                {
                    return false;
                }

                envelopeSends++;
                return envelopeSends == 1;
            };

            Check.Equal(SendStatus.Sent, client.SendReliable(40, new byte[] { 1 }), "reliable send should be accepted");

            var beforeResend = client.Metrics;
            Check.Equal(1, beforeResend.PendingCount, "one send should be pending right after SendReliable");
            Check.Equal(1L, beforeResend.ReliableSent, "one reliable send should be counted");
            Check.Equal(0L, beforeResend.ReliableResent, "no resend has happened yet");
            Check.Equal(0L, beforeResend.Delivered, "not delivered yet");

            Pipe.Exchange(clientWire, serverWire);
            server.Tick(now);

            now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
            client.Tick(now);
            Pipe.Exchange(clientWire, serverWire);
            var serverEvents = server.Tick(now);
            Check.True(FindMessage(serverEvents, 40).HasValue, "the resent envelope should reach the app");

            var serverMetrics = server.Metrics;
            Check.Equal(1L, serverMetrics.EnvelopesReceived, "server should have observed exactly one envelope arrival");
            Check.Equal(0L, serverMetrics.DuplicatesDropped, "the first arrival is not a duplicate");
            Check.Equal(1L, serverMetrics.AcksSent, "server should have acked the arrival");

            Pipe.Exchange(clientWire, serverWire);
            var afterAck = client.Tick(now);
            Check.True(Has(afterAck, ReliableEventKind.Delivered, 40), "sender should see Delivered");

            var afterMetrics = client.Metrics;
            Check.Equal(0, afterMetrics.PendingCount, "nothing should still be pending after delivery");
            Check.Equal(1L, afterMetrics.ReliableSent, "ReliableSent counts the logical send once, not per attempt");
            Check.Equal(1L, afterMetrics.ReliableResent, "exactly one resend happened");
            Check.Equal(1L, afterMetrics.Delivered, "exactly one delivery should be counted");
            Check.Equal(0L, afterMetrics.DeliveryFailed, "this message was delivered, not failed");
        }

        public static void MetricsTrackRejectionsAndFailures()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(20),
                MaxAttempts = 2,
                MaxPendingSends = 1,
            };
            var (client, _, clientWire, _, now) = MakeReadyPair(config);

            clientWire.DropOutgoing = frame =>
                FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.EnvelopeMessageId;

            Check.Equal(SendStatus.Sent, client.SendReliable(50, new byte[] { 1 }), "first reliable send should be accepted");
            Check.Equal(SendStatus.Congested, client.SendReliable(51, new byte[] { 2 }), "second send should be refused, MaxPendingSends is 1");

            var collected = new List<ReliableEvent>();
            for (int step = 0; step < 6 && !Has(collected, ReliableEventKind.DeliveryFailed, 50); step++)
            {
                now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
                collected.AddRange(client.Tick(now));
            }

            var metrics = client.Metrics;
            Check.Equal(1L, metrics.RejectedPendingFull, "exactly one send should have been rejected for a full pending table");
            Check.Equal(1L, metrics.DeliveryFailed, "the never-acked send should have failed exactly once");
            Check.Equal(0, metrics.PendingCount, "the failed send should no longer be pending");
        }

        public static void GivesUpAfterMaxAttempts()
        {
            var config = new ReliabilityConfig
            {
                ResendInterval = TimeSpan.FromMilliseconds(20),
                MaxAttempts = 3,
            };
            var (client, _, clientWire, _, now) = MakeReadyPair(config);

            clientWire.DropOutgoing = frame =>
                FrameSniff.TryMessageId(frame, out var id) && id == ReliabilitySchema.EnvelopeMessageId;

            var status = client.SendReliable(4, new byte[] { 1 });
            Check.Equal(SendStatus.Sent, status, "reliable send should be accepted locally");

            var collected = new List<ReliableEvent>();
            for (int step = 0; step < 8 && !Has(collected, ReliableEventKind.DeliveryFailed, 4); step++)
            {
                now += config.ResendInterval + TimeSpan.FromMilliseconds(5);
                collected.AddRange(client.Tick(now));
            }

            Check.True(Has(collected, ReliableEventKind.DeliveryFailed, 4), "delivery should be reported failed after max attempts");
        }
    }
}
