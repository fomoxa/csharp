using System;

namespace Fomoxa.Reliability
{
    public sealed class ReliabilityConfig
    {
        public TimeSpan ResendInterval { get; set; } = TimeSpan.FromMilliseconds(200);

        public int MaxAttempts { get; set; } = 10;

        public int DedupeWindowSize { get; set; } = 256;

        /// Ceiling on how many reliable sends may be waiting for an Ack at once.
        /// SendReliable refuses (SendStatus.Congested) once this is reached,
        /// rather than letting the pending table grow without a bound.
        public int MaxPendingSends { get; set; } = 256;

        /// Ceiling on how many times this side will re-send an Ack for the same
        /// inbound Seq. A legitimate retry (lost Ack) needs at most MaxAttempts
        /// re-acks from the sender's own retry budget, so this should stay
        /// comfortably above the peer's MaxAttempts; past it, further duplicates
        /// are still deduped but no longer get a reply.
        public int MaxAcksPerMessage { get; set; } = 16;

        public ReliabilityConfig Clone() =>
            new ReliabilityConfig
            {
                ResendInterval = ResendInterval,
                MaxAttempts = MaxAttempts,
                DedupeWindowSize = DedupeWindowSize,
                MaxPendingSends = MaxPendingSends,
                MaxAcksPerMessage = MaxAcksPerMessage,
            };
    }
}
