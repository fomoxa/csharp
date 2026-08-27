using System;
using System.Collections.Generic;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Reliability
{
    /// Server-side counterpart of ReliableChannel: wraps one FomoxaServer and
    /// keeps one ReliabilityPeer per connected peer id, torn down when that peer
    /// disconnects or fails handshake. Does not touch FomoxaServer/FomoxaConnection.
    public sealed class ReliableServer : IDisposable
    {
        private readonly FomoxaServer server;
        private readonly ReliabilityConfig config;
        private readonly Dictionary<ulong, ReliabilityPeer> peers = new Dictionary<ulong, ReliabilityPeer>();
        private readonly List<ReliableEvent> outEvents = new List<ReliableEvent>();
        private readonly List<uint> dueScratch = new List<uint>();
        private readonly List<uint> failedScratch = new List<uint>();
        private readonly byte[] ackScratch = new byte[ReliabilityWire.AckSize];

        // Same reasoning as ReliableChannel: resend bookkeeping must use the
        // time the caller passed into Tick, not a clock read on the spot, or an
        // app driving Tick with a fake/manual clock for deterministic tests
        // would see SendReliable record a real wall-clock LastSent that
        // CollectDue's fake "now" never catches up to.
        private TimeSpan lastTickNow;

        public ReliableServer(
            IListenerTransport listener,
            Schema appSchema,
            SessionConfig sessionConfig,
            ReliabilityConfig? config = null)
        {
            this.config = (config ?? new ReliabilityConfig()).Clone();
            server = new FomoxaServer(listener, ReliabilitySchema.Combine(appSchema), sessionConfig);
        }

        public int PeerCount => server.PeerCount;

        /// Number of peers this wrapper still holds reliability bookkeeping
        /// (pending sends, dedupe window) for. Exposed mainly so a caller - or a
        /// test - can confirm that bookkeeping is actually torn down on
        /// disconnect rather than leaking, since PeerCount alone only reflects
        /// FomoxaServer's own connection list.
        public int TrackedPeerCount => peers.Count;

        public FomoxaServer Server => server;

        public IReadOnlyList<ulong> PeerIds() => server.PeerIds();

        public ReliabilityMetrics? PeerMetrics(ulong peerId) =>
            peers.TryGetValue(peerId, out var peer) ? peer.Snapshot() : (ReliabilityMetrics?)null;

        /// Sum of every currently-tracked peer's metrics. A peer that has
        /// already disconnected contributes nothing - its counters leave with
        /// it, same as the rest of its reliability state.
        public ReliabilityMetrics AggregateMetrics()
        {
            int pendingCount = 0;
            long reliableSent = 0;
            long reliableResent = 0;
            long delivered = 0;
            long deliveryFailed = 0;
            long rejectedPendingFull = 0;
            long envelopesReceived = 0;
            long duplicatesDropped = 0;
            long acksSent = 0;
            long acksSuppressed = 0;

            foreach (var entry in peers)
            {
                var snapshot = entry.Value.Snapshot();
                pendingCount += snapshot.PendingCount;
                reliableSent += snapshot.ReliableSent;
                reliableResent += snapshot.ReliableResent;
                delivered += snapshot.Delivered;
                deliveryFailed += snapshot.DeliveryFailed;
                rejectedPendingFull += snapshot.RejectedPendingFull;
                envelopesReceived += snapshot.EnvelopesReceived;
                duplicatesDropped += snapshot.DuplicatesDropped;
                acksSent += snapshot.AcksSent;
                acksSuppressed += snapshot.AcksSuppressed;
            }

            return new ReliabilityMetrics(
                pendingCount,
                reliableSent,
                reliableResent,
                delivered,
                deliveryFailed,
                rejectedPendingFull,
                envelopesReceived,
                duplicatesDropped,
                acksSent,
                acksSuppressed);
        }

        public SendStatus SendUnreliable(ulong peerId, uint messageId, ReadOnlySpan<byte> payload) =>
            server.Send(peerId, messageId, payload);

        public SendStatus SendReliable(ulong peerId, uint messageId, ReadOnlySpan<byte> payload)
        {
            var peer = GetOrCreatePeer(peerId);
            if (!peer.CanAcceptNewSend())
            {
                return SendStatus.Congested;
            }

            var envelope = peer.PrepareReliableSend(messageId, payload, out var seq);
            var status = server.Send(peerId, ReliabilitySchema.EnvelopeMessageId, envelope);

            if (status == SendStatus.Sent || status == SendStatus.Congested || status == SendStatus.NotReady)
            {
                peer.RegisterPending(seq, messageId, envelope, lastTickNow, status == SendStatus.Sent);
            }

            return status;
        }

        public void BroadcastUnreliable(uint messageId, ReadOnlySpan<byte> payload) =>
            server.Broadcast(messageId, payload);

        public void BroadcastReliable(uint messageId, ReadOnlySpan<byte> payload)
        {
            var ids = server.PeerIds();
            for (int index = 0; index < ids.Count; index++)
            {
                SendReliable(ids[index], messageId, payload);
            }
        }

        public void Disconnect(ulong peerId) => server.Disconnect(peerId);

        public IReadOnlyList<ReliableEvent> Tick(TimeSpan now)
        {
            lastTickNow = now;
            outEvents.Clear();
            ResendDue(now);

            var raised = server.Tick(now);
            for (int index = 0; index < raised.Count; index++)
            {
                Dispatch(raised[index]);
            }

            return outEvents;
        }

        public IReadOnlyList<ReliableEvent> Tick() => Tick(MonotonicClock.Now);

        private void ResendDue(TimeSpan now)
        {
            foreach (var entry in peers)
            {
                dueScratch.Clear();
                failedScratch.Clear();
                entry.Value.CollectDue(now, dueScratch);

                for (int index = 0; index < dueScratch.Count; index++)
                {
                    var seq = dueScratch[index];
                    var envelope = entry.Value.Envelope(seq);
                    if (envelope == null)
                    {
                        continue;
                    }

                    var status = server.Send(entry.Key, ReliabilitySchema.EnvelopeMessageId, envelope);
                    entry.Value.ReportAttempt(seq, status == SendStatus.Sent, now, failedScratch);
                }

                for (int index = 0; index < failedScratch.Count; index++)
                {
                    outEvents.Add(ReliableEvent.DeliveryFailed(entry.Key, failedScratch[index]));
                }
            }
        }

        private void Dispatch(FomoxaEvent raw)
        {
            switch (raw.Kind)
            {
                case FomoxaEventKind.Connected:
                    GetOrCreatePeer(raw.PeerId);
                    outEvents.Add(ReliableEvent.Connected(raw.PeerId));
                    break;

                case FomoxaEventKind.Ready:
                    outEvents.Add(ReliableEvent.Ready(raw.PeerId));
                    break;

                case FomoxaEventKind.HandshakeFailed:
                    peers.Remove(raw.PeerId);
                    outEvents.Add(ReliableEvent.HandshakeFailed(raw.PeerId, raw.Failure));
                    break;

                case FomoxaEventKind.Disconnected:
                    peers.Remove(raw.PeerId);
                    outEvents.Add(ReliableEvent.Disconnected(raw.PeerId, raw.Reason));
                    break;

                case FomoxaEventKind.Message:
                    HandleMessage(raw);
                    break;
            }
        }

        private void HandleMessage(FomoxaEvent raw)
        {
            var peer = GetOrCreatePeer(raw.PeerId);

            if (raw.MessageId == ReliabilitySchema.AckMessageId)
            {
                if (ReliabilityWire.TryDecodeAck(raw.Payload.Span, out var seq) &&
                    peer.AcknowledgeAndTakeDelivered(seq, out var deliveredId))
                {
                    outEvents.Add(ReliableEvent.Delivered(raw.PeerId, deliveredId));
                }

                return;
            }

            if (raw.MessageId == ReliabilitySchema.EnvelopeMessageId)
            {
                if (!ReliabilityWire.TryDecodeEnvelopeHeader(
                        raw.Payload.Span, out var seq, out var innerId, out var innerLength))
                {
                    return;
                }

                var observation = peer.ObserveEnvelope(seq);

                if (observation.ShouldAck)
                {
                    ReliabilityWire.EncodeAckInto(ackScratch, seq);
                    server.Send(raw.PeerId, ReliabilitySchema.AckMessageId, ackScratch);
                }

                if (observation.IsNew)
                {
                    var innerPayload = raw.Payload.Slice(ReliabilityWire.EnvelopeHeaderSize, innerLength);
                    outEvents.Add(ReliableEvent.Message(raw.PeerId, innerId, innerPayload, wasReliable: true));
                }

                return;
            }

            outEvents.Add(ReliableEvent.Message(raw.PeerId, raw.MessageId, raw.Payload, wasReliable: false));
        }

        private ReliabilityPeer GetOrCreatePeer(ulong peerId)
        {
            if (!peers.TryGetValue(peerId, out var peer))
            {
                peer = new ReliabilityPeer(config);
                peers[peerId] = peer;
            }

            return peer;
        }

        public void Dispose() => server.Dispose();
    }
}
