namespace Fomoxa.Reliability
{
    /// A point-in-time snapshot of one peer's reliability bookkeeping. Meant to
    /// be read periodically (a debug overlay polling once a second, a log line
    /// on disconnect), not every tick - every field here is a plain counter
    /// already maintained on the hot path, so taking a snapshot costs nothing
    /// beyond copying it, but polling it in a loop teaches you nothing new
    /// between calls.
    public readonly struct ReliabilityMetrics
    {
        public ReliabilityMetrics(
            int pendingCount,
            long reliableSent,
            long reliableResent,
            long delivered,
            long deliveryFailed,
            long rejectedPendingFull,
            long envelopesReceived,
            long duplicatesDropped,
            long acksSent,
            long acksSuppressed)
        {
            PendingCount = pendingCount;
            ReliableSent = reliableSent;
            ReliableResent = reliableResent;
            Delivered = delivered;
            DeliveryFailed = deliveryFailed;
            RejectedPendingFull = rejectedPendingFull;
            EnvelopesReceived = envelopesReceived;
            DuplicatesDropped = duplicatesDropped;
            AcksSent = acksSent;
            AcksSuppressed = acksSuppressed;
        }

        /// Reliable sends awaiting an Ack right now. The only field here that
        /// is not cumulative - a rising trend here is the first sign of a peer
        /// that is not keeping up.
        public int PendingCount { get; }

        /// SendReliable calls accepted and registered for tracking.
        public long ReliableSent { get; }

        /// Actual retransmissions beyond each message's first send attempt.
        /// High relative to ReliableSent means the link is lossy or MaxAttempts/
        /// ResendInterval are tuned too aggressively for it.
        public long ReliableResent { get; }

        /// Reliable sends confirmed delivered by an Ack.
        public long Delivered { get; }

        /// Reliable sends that exhausted MaxAttempts without an Ack. Should stay
        /// at zero in normal operation; a nonzero rate means real, lasting loss.
        public long DeliveryFailed { get; }

        /// SendReliable calls refused because MaxPendingSends was already
        /// reached. Nonzero means the app is issuing reliable sends faster than
        /// they can be acked, or MaxPendingSends is set too low for that rate.
        public long RejectedPendingFull { get; }

        /// Inbound envelope arrivals, new or duplicate.
        public long EnvelopesReceived { get; }

        /// Inbound envelope arrivals that were duplicates and not delivered to
        /// the app again. A nonzero rate is expected under loss - it means the
        /// resend mechanism is doing its job, not that something is wrong.
        public long DuplicatesDropped { get; }

        /// Ack frames actually sent in reply to an inbound envelope.
        public long AcksSent { get; }

        /// Ack replies skipped because MaxAcksPerMessage was already spent for
        /// that Seq. Nonzero means a peer kept resending well past what a
        /// normal lost-Ack retry needs.
        public long AcksSuppressed { get; }
    }
}
