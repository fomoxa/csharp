using System;
using System.Buffers.Binary;

namespace Fomoxa.Net.Framing
{
    internal enum AssemblyStatus
    {
        Pending,
        Complete,
        Broken,
    }

    internal sealed class HandshakeAssembler
    {
        private const int PartFieldsSize = FomoxaWire.HandshakePartHeaderSize - 1;

        private byte[] content = Array.Empty<byte>();
        private bool[] received = Array.Empty<bool>();
        private int count;
        private int receivedCount;
        private int lastLength;

        public bool IsPending => count > 0;

        public ReadOnlySpan<byte> Content =>
            content.AsSpan(0, ((count - 1) * FomoxaWire.HandshakePartContent) + lastLength);

        public AssemblyStatus Add(ReadOnlySpan<byte> part)
        {
            int index = BinaryPrimitives.ReadUInt16LittleEndian(part.Slice(0, 2));
            int declaredCount = BinaryPrimitives.ReadUInt16LittleEndian(part.Slice(2, 2));
            var bytes = part.Slice(PartFieldsSize);

            if (declaredCount < 2 || declaredCount > FomoxaWire.MaxHandshakeParts || index >= declaredCount)
            {
                return AssemblyStatus.Broken;
            }
            bool last = index == declaredCount - 1;
            if (last ? bytes.Length == 0 : bytes.Length != FomoxaWire.HandshakePartContent)
            {
                return AssemblyStatus.Broken;
            }

            if (count == 0)
            {
                Begin(declaredCount);
            }
            else if (declaredCount != count)
            {
                return AssemblyStatus.Broken;
            }

            int offset = index * FomoxaWire.HandshakePartContent;
            if (received[index])
            {
                bool same = (!last || bytes.Length == lastLength)
                    && bytes.SequenceEqual(content.AsSpan(offset, bytes.Length));
                return same ? AssemblyStatus.Pending : AssemblyStatus.Broken;
            }

            bytes.CopyTo(content.AsSpan(offset));
            received[index] = true;
            receivedCount++;
            if (last)
            {
                lastLength = bytes.Length;
            }

            if (receivedCount < count)
            {
                return AssemblyStatus.Pending;
            }
            return Content.Length > FomoxaWire.MaxHandshakePayload ? AssemblyStatus.Broken : AssemblyStatus.Complete;
        }

        public void Reset()
        {
            if (count > 0)
            {
                Array.Clear(received, 0, count);
            }
            count = 0;
            receivedCount = 0;
            lastLength = 0;
        }

        public void ShrinkToFit()
        {
            if (count == 0)
            {
                content = Array.Empty<byte>();
                received = Array.Empty<bool>();
            }
        }

        private void Begin(int declaredCount)
        {
            int capacity = declaredCount * FomoxaWire.HandshakePartContent;
            if (content.Length < capacity)
            {
                content = new byte[capacity];
            }
            if (received.Length < declaredCount)
            {
                received = new bool[declaredCount];
            }
            count = declaredCount;
        }
    }
}
