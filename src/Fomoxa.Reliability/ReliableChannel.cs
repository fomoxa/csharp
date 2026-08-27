using System;
using System.Collections.Generic;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Reliability
{
    /// Wraps one FomoxaConnection running on a Message-kind (unreliable) transport
    /// and adds a per-send choice between SendUnreliable (straight pass-through,
    /// like Fomoxa's own Send) and SendReliable (wrapped in an envelope, resent
    /// until the peer's Ack arrives). Does not touch Fomoxa.Net or ITransport -
    /// everything here is ordinary use of FomoxaConnection's public API.
    public sealed class ReliableChannel : IDisposable
    {
        private readonly FomoxaConnection connection;
        private readonly ReliabilityPeer peer;
        private readonly List<ReliableEvent> outEvents = new List<ReliableEvent>();
        private readonly List<uint> dueScratch = new List<uint>();
        private readonly List<uint> failedScratch = new List<uint>();
        private readonly byte[] ackScratch = new byte[ReliabilityWire.AckSize];

        // The time source for all resend bookkeeping must be whatever the
        // caller passes into Tick, never a clock read internally - otherwise an
        // app driving Tick with a fake/manual clock (to test timeouts without
        // sleeping) would see SendReliable record a real wall-clock LastSent
        // that CollectDue's fake "now" can never catch up to, so nothing would
        // ever be resent. Defaults to TimeSpan.Zero, which is always already
        // "due" - biased towards an eager first retry, never towards silently
        // never retrying, if SendReliable is somehow called before any Tick.
        private TimeSpan lastTickNow;

        public ReliableChannel(FomoxaConnection connection, ReliabilityConfig? config = null)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            peer = new ReliabilityPeer((config ?? new ReliabilityConfig()).Clone());
        }

        /// appSchema should be the application's own message set; the envelope
        /// and ack control messages are added on top by ReliabilitySchema.Combine.
        public static ReliableChannel Connect(
            ITransport transport,
            Schema appSchema,
            SessionConfig sessionConfig,
            ReliabilityConfig? reliabilityConfig = null)
        {
            var combined = ReliabilitySchema.Combine(appSchema);
            var connection = FomoxaConnection.Connect(transport, combined, sessionConfig);
            return new ReliableChannel(connection, reliabilityConfig);
        }

        public bool IsReady => connection.IsReady;

        public bool IsClosed => connection.IsClosed;

        public ulong PeerId => connection.PeerId;

        public FomoxaConnection Connection => connection;

        public ReliabilityMetrics Metrics => peer.Snapshot();

        public SendStatus SendUnreliable(uint messageId, ReadOnlySpan<byte> payload) =>
            connection.Send(messageId, payload);

        public SendStatus SendReliable(uint messageId, ReadOnlySpan<byte> payload)
        {
            if (!peer.CanAcceptNewSend())
            {
                return SendStatus.Congested;
            }

            var envelope = peer.PrepareReliableSend(messageId, payload, out var seq);
            var status = connection.Send(ReliabilitySchema.EnvelopeMessageId, envelope);

            // Congested/NotReady mean the frame never left the machine right now,
            // not that it never will - it still has to be tracked for retry, or a
            // reliable send made while the link is briefly busy would vanish with
            // no resend and no DeliveryFailed to explain why.
            if (status == SendStatus.Sent || status == SendStatus.Congested || status == SendStatus.NotReady)
            {
                peer.RegisterPending(seq, messageId, envelope, lastTickNow, status == SendStatus.Sent);
            }

            return status;
        }

        public void Close() => connection.Close();

        public IReadOnlyList<ReliableEvent> Tick(TimeSpan now)
        {
            lastTickNow = now;
            outEvents.Clear();
            ResendDue(now);

            var raised = connection.Tick(now);
            for (int index = 0; index < raised.Count; index++)
            {
                Dispatch(raised[index]);
            }

            return outEvents;
        }

        public IReadOnlyList<ReliableEvent> Tick() => Tick(MonotonicClock.Now);

        private void ResendDue(TimeSpan now)
        {
            dueScratch.Clear();
            failedScratch.Clear();
            peer.CollectDue(now, dueScratch);

            for (int index = 0; index < dueScratch.Count; index++)
            {
                var seq = dueScratch[index];
                var envelope = peer.Envelope(seq);
                if (envelope == null)
                {
                    continue;
                }

                var status = connection.Send(ReliabilitySchema.EnvelopeMessageId, envelope);
                peer.ReportAttempt(seq, status == SendStatus.Sent, now, failedScratch);
            }

            for (int index = 0; index < failedScratch.Count; index++)
            {
                outEvents.Add(ReliableEvent.DeliveryFailed(connection.PeerId, failedScratch[index]));
            }
        }

        private void Dispatch(FomoxaEvent raw)
        {
            switch (raw.Kind)
            {
                case FomoxaEventKind.Connected:
                    outEvents.Add(ReliableEvent.Connected(raw.PeerId));
                    break;

                case FomoxaEventKind.Ready:
                    outEvents.Add(ReliableEvent.Ready(raw.PeerId));
                    break;

                case FomoxaEventKind.HandshakeFailed:
                    outEvents.Add(ReliableEvent.HandshakeFailed(raw.PeerId, raw.Failure));
                    break;

                case FomoxaEventKind.Disconnected:
                    peer.Reset();
                    outEvents.Add(ReliableEvent.Disconnected(raw.PeerId, raw.Reason));
                    break;

                case FomoxaEventKind.Message:
                    HandleMessage(raw);
                    break;
            }
        }

        private void HandleMessage(FomoxaEvent raw)
        {
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
                    connection.Send(ReliabilitySchema.AckMessageId, ackScratch);
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

        public void Dispose() => connection.Dispose();
    }
}
