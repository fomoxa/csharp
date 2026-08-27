using System;
using System.Collections.Generic;

namespace Fomoxa.Reliability
{
    internal sealed class PendingSend
    {
        public uint MessageId;
        public byte[] Envelope = Array.Empty<byte>();
        public TimeSpan LastSent;
        public int Attempts;
    }

    /// Per-peer state for the reliable-envelope mechanism: outstanding sends
    /// waiting for an Ack, and the dedupe window for inbound envelopes. One
    /// instance per logical peer - ReliableChannel owns exactly one, ReliableServer
    /// keeps one per connected peer id.
    internal sealed class ReliabilityPeer
    {
        private readonly ReliabilityConfig config;
        private readonly Dictionary<uint, PendingSend> pending = new Dictionary<uint, PendingSend>();
        private readonly SeenWindow seen;
        private uint nextSeq = 1;

        private long reliableSent;
        private long reliableResent;
        private long delivered;
        private long deliveryFailed;
        private long rejectedPendingFull;
        private long envelopesReceived;
        private long duplicatesDropped;
        private long acksSent;
        private long acksSuppressed;

        public ReliabilityPeer(ReliabilityConfig config)
        {
            this.config = config;
            seen = new SeenWindow(config.DedupeWindowSize);
        }

        public byte[] PrepareReliableSend(uint messageId, ReadOnlySpan<byte> payload, out uint seq)
        {
            seq = nextSeq++;
            return ReliabilityWire.EncodeEnvelope(seq, messageId, payload);
        }

        /// actuallySent must be false when the local Send attempt only came back
        /// Congested/NotReady rather than truly reaching the transport (nothing
        /// left the machine). That case still needs to be tracked for retry, but
        /// must not spend one of the MaxAttempts tries, and should be retried on
        /// the very next tick rather than waiting a full ResendInterval.
        public void RegisterPending(uint seq, uint messageId, byte[] envelope, TimeSpan now, bool actuallySent)
        {
            pending[seq] = new PendingSend
            {
                MessageId = messageId,
                Envelope = envelope,
                LastSent = actuallySent ? now : now - config.ResendInterval,
                Attempts = actuallySent ? 1 : 0,
            };
            reliableSent++;
        }

        public bool AcknowledgeAndTakeDelivered(uint seq, out uint deliveredMessageId)
        {
            if (pending.TryGetValue(seq, out var found))
            {
                pending.Remove(seq);
                deliveredMessageId = found.MessageId;
                delivered++;
                return true;
            }

            deliveredMessageId = 0;
            return false;
        }

        public bool CanAcceptNewSend()
        {
            if (pending.Count < config.MaxPendingSends)
            {
                return true;
            }

            rejectedPendingFull++;
            return false;
        }

        public EnvelopeObservation ObserveEnvelope(uint seq)
        {
            var observation = seen.Observe(seq, config.MaxAcksPerMessage);
            envelopesReceived++;
            if (!observation.IsNew)
            {
                duplicatesDropped++;
            }

            if (observation.ShouldAck)
            {
                acksSent++;
            }
            else
            {
                acksSuppressed++;
            }

            return observation;
        }

        public void CollectDue(TimeSpan now, List<uint> dueSeqs)
        {
            foreach (var entry in pending)
            {
                if (now - entry.Value.LastSent >= config.ResendInterval)
                {
                    dueSeqs.Add(entry.Key);
                }
            }
        }

        public byte[]? Envelope(uint seq) => pending.TryGetValue(seq, out var send) ? send.Envelope : null;

        /// Reports what actually happened when a due entry was (re)sent. A
        /// merely-congested attempt (nothing left the machine) does not count
        /// against MaxAttempts and stays due immediately on the next tick.
        public void ReportAttempt(uint seq, bool actuallySent, TimeSpan now, List<uint> failedMessageIds)
        {
            if (!actuallySent || !pending.TryGetValue(seq, out var send))
            {
                return;
            }

            send.Attempts++;
            send.LastSent = now;
            reliableResent++;
            if (send.Attempts >= config.MaxAttempts)
            {
                pending.Remove(seq);
                deliveryFailed++;
                failedMessageIds.Add(send.MessageId);
            }
        }

        public void Reset()
        {
            pending.Clear();
        }

        public ReliabilityMetrics Snapshot() =>
            new ReliabilityMetrics(
                pending.Count,
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
}
