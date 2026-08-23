using System;
using System.Buffers.Binary;

namespace Fomoxa.Net.Framing
{
    internal enum FrameScan
    {
        Ok,
        Incomplete,
        Corrupt,
    }

    internal static class FrameLayout
    {
        public static FrameScan Scan(
            ReadOnlySpan<byte> bytes,
            out FrameType type,
            out uint messageId,
            out int headerSize,
            out int payloadLength)
        {
            type = FrameType.Data;
            messageId = 0;
            headerSize = 0;
            payloadLength = 0;

            if (bytes.Length < 1)
            {
                return FrameScan.Incomplete;
            }

            switch (bytes[0])
            {
                case (byte)FrameType.Data:
                {
                    if (bytes.Length < FomoxaWire.DataFrameHeaderSize)
                    {
                        return FrameScan.Incomplete;
                    }
                    if (bytes[1] != FomoxaWire.DataMagicFirst || bytes[2] != FomoxaWire.DataMagicSecond)
                    {
                        return FrameScan.Corrupt;
                    }
                    uint declared = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(7, 4));
                    if (declared > FomoxaWire.MaxMessagePayload)
                    {
                        return FrameScan.Corrupt;
                    }
                    type = FrameType.Data;
                    messageId = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(3, 4));
                    headerSize = FomoxaWire.DataFrameHeaderSize;
                    payloadLength = (int)declared;
                    return bytes.Length < headerSize + payloadLength ? FrameScan.Incomplete : FrameScan.Ok;
                }

                case (byte)FrameType.Ping:
                    type = FrameType.Ping;
                    headerSize = 1;
                    payloadLength = 0;
                    return FrameScan.Ok;

                case (byte)FrameType.Pong:
                    type = FrameType.Pong;
                    headerSize = 1;
                    payloadLength = 0;
                    return FrameScan.Ok;

                case (byte)FrameType.Handshake:
                {
                    if (bytes.Length < FomoxaWire.HandshakeFrameHeaderSize)
                    {
                        return FrameScan.Incomplete;
                    }
                    uint declared = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(1, 4));
                    if (declared > FomoxaWire.MaxHandshakePayload)
                    {
                        return FrameScan.Corrupt;
                    }
                    type = FrameType.Handshake;
                    headerSize = FomoxaWire.HandshakeFrameHeaderSize;
                    payloadLength = (int)declared;
                    return bytes.Length < headerSize + payloadLength ? FrameScan.Incomplete : FrameScan.Ok;
                }

                default:
                    return FrameScan.Corrupt;
            }
        }

        public static int DataFrameSize(int payloadLength) =>
            FomoxaWire.DataFrameHeaderSize + payloadLength;

        public static int HandshakeFrameSize(int payloadLength) =>
            FomoxaWire.HandshakeFrameHeaderSize + payloadLength;

        public static void WriteDataFrame(Span<byte> destination, uint messageId, ReadOnlySpan<byte> payload)
        {
            destination[0] = (byte)FrameType.Data;
            destination[1] = FomoxaWire.DataMagicFirst;
            destination[2] = FomoxaWire.DataMagicSecond;
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(3, 4), messageId);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(7, 4), (uint)payload.Length);
            payload.CopyTo(destination.Slice(FomoxaWire.DataFrameHeaderSize));
        }

        public static void WriteHandshakeHeader(Span<byte> destination, int payloadLength)
        {
            destination[0] = (byte)FrameType.Handshake;
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), (uint)payloadLength);
        }

        public static void WritePing(Span<byte> destination) => destination[0] = (byte)FrameType.Ping;

        public static void WritePong(Span<byte> destination) => destination[0] = (byte)FrameType.Pong;
    }
}
