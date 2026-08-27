using System.Buffers.Binary;

namespace Fomoxa.Reliability
{
    internal static class ReliabilityWire
    {
        public const int EnvelopeHeaderSize = 12;
        public const int AckSize = 4;

        public static byte[] EncodeEnvelope(uint seq, uint innerMessageId, System.ReadOnlySpan<byte> innerPayload)
        {
            var buffer = new byte[EnvelopeHeaderSize + innerPayload.Length];
            var span = new System.Span<byte>(buffer);
            BinaryPrimitives.WriteUInt32LittleEndian(span, seq);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4), innerMessageId);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(8), (uint)innerPayload.Length);
            innerPayload.CopyTo(span.Slice(EnvelopeHeaderSize));
            return buffer;
        }

        public static bool TryDecodeEnvelopeHeader(
            System.ReadOnlySpan<byte> data, out uint seq, out uint innerMessageId, out int innerLength)
        {
            if (data.Length < EnvelopeHeaderSize)
            {
                seq = 0;
                innerMessageId = 0;
                innerLength = 0;
                return false;
            }

            seq = BinaryPrimitives.ReadUInt32LittleEndian(data);
            innerMessageId = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(4));
            uint declaredLength = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(8));

            long remaining = data.Length - EnvelopeHeaderSize;
            if (declaredLength > int.MaxValue || declaredLength != remaining)
            {
                innerLength = 0;
                return false;
            }

            innerLength = (int)declaredLength;
            return true;
        }

        /// Writes into a caller-owned buffer instead of allocating: an Ack is
        /// sent on every inbound envelope arrival, including duplicates up to
        /// MaxAcksPerMessage, so this runs far more often than SendReliable
        /// itself and is worth keeping allocation-free.
        public static void EncodeAckInto(System.Span<byte> destination, uint seq)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination, seq);
        }

        public static bool TryDecodeAck(System.ReadOnlySpan<byte> data, out uint seq)
        {
            if (data.Length != AckSize)
            {
                seq = 0;
                return false;
            }

            seq = BinaryPrimitives.ReadUInt32LittleEndian(data);
            return true;
        }
    }
}
