using System;

namespace Fomoxa.Net.Framing
{
    internal sealed class StreamFrameDecoder
    {
        private byte[] buffer = new byte[4096];
        private int start;
        private int end;
        private bool poisoned;

        public byte[] Buffer => buffer;

        public bool IsPoisoned => poisoned;

        public int Buffered => end - start;

        public void Push(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length == 0)
            {
                return;
            }
            EnsureRoom(bytes.Length);
            bytes.CopyTo(buffer.AsSpan(end));
            end += bytes.Length;
        }

        public FrameScan TryRead(out FrameType type, out uint messageId, out int payloadOffset, out int payloadLength)
        {
            type = FrameType.Data;
            messageId = 0;
            payloadOffset = 0;
            payloadLength = 0;

            if (poisoned)
            {
                return FrameScan.Corrupt;
            }

            var window = buffer.AsSpan(start, end - start);
            var scan = FrameLayout.Scan(window, out type, out messageId, out int headerSize, out int declared);
            switch (scan)
            {
                case FrameScan.Incomplete:
                    return FrameScan.Incomplete;

                case FrameScan.Corrupt:
                    poisoned = true;
                    return FrameScan.Corrupt;

                default:
                    payloadOffset = start + headerSize;
                    payloadLength = declared;
                    start += headerSize + declared;
                    if (start == end)
                    {
                        start = 0;
                        end = 0;
                    }
                    return FrameScan.Ok;
            }
        }

        private void EnsureRoom(int extra)
        {
            if (buffer.Length - end >= extra)
            {
                return;
            }

            int used = end - start;
            if (start > 0)
            {
                System.Buffer.BlockCopy(buffer, start, buffer, 0, used);
                start = 0;
                end = used;
            }
            if (buffer.Length - end >= extra)
            {
                return;
            }

            int capacity = buffer.Length;
            long needed = (long)used + extra;
            while (capacity < needed)
            {
                capacity *= 2;
            }
            var grown = new byte[capacity];
            System.Buffer.BlockCopy(buffer, 0, grown, 0, used);
            buffer = grown;
        }
    }
}
