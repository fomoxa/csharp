using System.Collections.Generic;

namespace Fomoxa.Reliability
{
    internal readonly struct EnvelopeObservation
    {
        public EnvelopeObservation(bool isNew, bool shouldAck)
        {
            IsNew = isNew;
            ShouldAck = shouldAck;
        }

        /// True the first time this Seq is observed - caller should deliver it to the app.
        public bool IsNew { get; }

        /// True while this Seq hasn't been re-acked past the configured cap yet.
        public bool ShouldAck { get; }
    }

    /// Bounded record of recently-seen inbound Seq values. Serves two purposes:
    /// dedupe delivery to the app (a retransmit racing its own Ack must not be
    /// delivered twice), and cap how many times a duplicate arrival is worth
    /// re-acking (a peer that keeps resending the same envelope well past what a
    /// real lost-Ack retry needs stops getting replies, instead of this side
    /// echoing an Ack forever). Oldest entry is evicted once capacity is
    /// exceeded - Seq is monotonic per sender, so a real duplicate always lands
    /// inside this window; a duplicate arriving long after eviction is treated
    /// as new again, which is the accepted cost of a bounded window.
    internal sealed class SeenWindow
    {
        private sealed class Entry
        {
            public int AckCount;
        }

        private readonly Dictionary<uint, Entry> seen = new Dictionary<uint, Entry>();
        private readonly Queue<uint> order = new Queue<uint>();
        private readonly int capacity;

        public SeenWindow(int capacity)
        {
            this.capacity = capacity < 1 ? 1 : capacity;
        }

        public EnvelopeObservation Observe(uint seq, int maxAcksPerMessage)
        {
            bool isNew = false;
            if (!seen.TryGetValue(seq, out var entry))
            {
                entry = new Entry();
                seen[seq] = entry;
                order.Enqueue(seq);
                if (order.Count > capacity)
                {
                    seen.Remove(order.Dequeue());
                }

                isNew = true;
            }

            bool shouldAck = entry.AckCount < maxAcksPerMessage;
            if (shouldAck)
            {
                entry.AckCount++;
            }

            return new EnvelopeObservation(isNew, shouldAck);
        }
    }
}
