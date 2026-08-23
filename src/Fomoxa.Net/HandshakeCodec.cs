using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Fomoxa.Net
{
    internal readonly struct AskItem
    {
        public AskItem(uint id, ushort fieldCount)
        {
            Id = id;
            FieldCount = fieldCount;
        }

        public uint Id { get; }

        public ushort FieldCount { get; }
    }

    internal static class HandshakeCodec
    {
        public static int HelloSize(Schema schema) =>
            FomoxaWire.HelloHeaderSize + (FomoxaWire.HelloEntrySize * schema.Messages.Count);

        public static void WriteHello(Span<byte> destination, Schema schema)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), FomoxaWire.ProtocolVersion);
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(4, 8), schema.Fingerprint);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(12, 4), (uint)schema.Messages.Count);

            int cursor = FomoxaWire.HelloHeaderSize;
            foreach (var message in schema.Messages)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), message.Id);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(cursor + 4, 2), message.FieldCount);
                BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(cursor + 6, 8), message.Fingerprint);
                cursor += FomoxaWire.HelloEntrySize;
            }
        }

        public static bool TryHelloCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < FomoxaWire.HelloHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(12, 4));
            if (declared > FomoxaWire.MaxHelloMessages)
            {
                return false;
            }
            long expected = FomoxaWire.HelloHeaderSize + ((long)FomoxaWire.HelloEntrySize * declared);
            if (payload.Length != expected)
            {
                return false;
            }
            count = (int)declared;
            return true;
        }

        public static uint HelloVersion(ReadOnlySpan<byte> payload) =>
            BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4));

        public static ulong HelloSchemaFingerprint(ReadOnlySpan<byte> payload) =>
            BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(4, 8));

        public static void HelloEntry(
            ReadOnlySpan<byte> payload, int index, out uint id, out ushort fieldCount, out ulong fingerprint)
        {
            int cursor = FomoxaWire.HelloHeaderSize + (index * FomoxaWire.HelloEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fieldCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(cursor + 4, 2));
            fingerprint = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(cursor + 6, 8));
        }

        public static int QuerySize(int itemCount) =>
            FomoxaWire.QueryHeaderSize + (FomoxaWire.QueryEntrySize * itemCount);

        public static void WriteQuery(Span<byte> destination, IReadOnlyList<AskItem> asks)
        {
            destination[0] = FomoxaWire.QueryVerdictByte;
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), (uint)asks.Count);

            int cursor = FomoxaWire.QueryHeaderSize;
            for (int index = 0; index < asks.Count; index++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), asks[index].Id);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(cursor + 4, 2), asks[index].FieldCount);
                cursor += FomoxaWire.QueryEntrySize;
            }
        }

        public static bool TryQueryCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < FomoxaWire.QueryHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(1, 4));
            if (declared > FomoxaWire.MaxQueryItems)
            {
                return false;
            }
            long expected = FomoxaWire.QueryHeaderSize + ((long)FomoxaWire.QueryEntrySize * declared);
            if (payload.Length != expected)
            {
                return false;
            }
            count = (int)declared;
            return true;
        }

        public static void QueryEntry(ReadOnlySpan<byte> payload, int index, out uint id, out ushort fieldCount)
        {
            int cursor = FomoxaWire.QueryHeaderSize + (index * FomoxaWire.QueryEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fieldCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(cursor + 4, 2));
        }

        public static int QueryReplySize(int itemCount) =>
            FomoxaWire.QueryReplyHeaderSize + (FomoxaWire.QueryReplyEntrySize * itemCount);

        public static void WriteQueryReplyHeader(Span<byte> destination, int itemCount) =>
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), (uint)itemCount);

        public static void WriteQueryReplyEntry(Span<byte> destination, int index, uint id, ulong fingerprint)
        {
            int cursor = FomoxaWire.QueryReplyHeaderSize + (index * FomoxaWire.QueryReplyEntrySize);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), id);
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(cursor + 4, 8), fingerprint);
        }

        public static bool TryQueryReplyCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < FomoxaWire.QueryReplyHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4));
            if (declared > FomoxaWire.MaxQueryItems)
            {
                return false;
            }
            long expected = FomoxaWire.QueryReplyHeaderSize + ((long)FomoxaWire.QueryReplyEntrySize * declared);
            if (payload.Length != expected)
            {
                return false;
            }
            count = (int)declared;
            return true;
        }

        public static void QueryReplyEntry(ReadOnlySpan<byte> payload, int index, out uint id, out ulong fingerprint)
        {
            int cursor = FomoxaWire.QueryReplyHeaderSize + (index * FomoxaWire.QueryReplyEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fingerprint = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(cursor + 4, 8));
        }
    }
}
