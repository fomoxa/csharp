using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Cyclone.Net
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
            CycloneWire.HelloHeaderSize + (CycloneWire.HelloEntrySize * schema.Messages.Count);

        public static void WriteHello(Span<byte> destination, Schema schema)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), CycloneWire.ProtocolVersion);
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(4, 8), schema.Fingerprint);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(12, 4), (uint)schema.Messages.Count);

            int cursor = CycloneWire.HelloHeaderSize;
            foreach (var message in schema.Messages)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), message.Id);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(cursor + 4, 2), message.FieldCount);
                BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(cursor + 6, 8), message.Fingerprint);
                cursor += CycloneWire.HelloEntrySize;
            }
        }

        public static bool TryHelloCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < CycloneWire.HelloHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(12, 4));
            if (declared > CycloneWire.MaxHelloMessages)
            {
                return false;
            }
            long expected = CycloneWire.HelloHeaderSize + ((long)CycloneWire.HelloEntrySize * declared);
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
            int cursor = CycloneWire.HelloHeaderSize + (index * CycloneWire.HelloEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fieldCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(cursor + 4, 2));
            fingerprint = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(cursor + 6, 8));
        }

        public static int QuerySize(int itemCount) =>
            CycloneWire.QueryHeaderSize + (CycloneWire.QueryEntrySize * itemCount);

        public static void WriteQuery(Span<byte> destination, IReadOnlyList<AskItem> asks)
        {
            destination[0] = CycloneWire.QueryVerdictByte;
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), (uint)asks.Count);

            int cursor = CycloneWire.QueryHeaderSize;
            for (int index = 0; index < asks.Count; index++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), asks[index].Id);
                BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(cursor + 4, 2), asks[index].FieldCount);
                cursor += CycloneWire.QueryEntrySize;
            }
        }

        public static bool TryQueryCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < CycloneWire.QueryHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(1, 4));
            if (declared > CycloneWire.MaxQueryItems)
            {
                return false;
            }
            long expected = CycloneWire.QueryHeaderSize + ((long)CycloneWire.QueryEntrySize * declared);
            if (payload.Length != expected)
            {
                return false;
            }
            count = (int)declared;
            return true;
        }

        public static void QueryEntry(ReadOnlySpan<byte> payload, int index, out uint id, out ushort fieldCount)
        {
            int cursor = CycloneWire.QueryHeaderSize + (index * CycloneWire.QueryEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fieldCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(cursor + 4, 2));
        }

        public static int QueryReplySize(int itemCount) =>
            CycloneWire.QueryReplyHeaderSize + (CycloneWire.QueryReplyEntrySize * itemCount);

        public static void WriteQueryReplyHeader(Span<byte> destination, int itemCount) =>
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), (uint)itemCount);

        public static void WriteQueryReplyEntry(Span<byte> destination, int index, uint id, ulong fingerprint)
        {
            int cursor = CycloneWire.QueryReplyHeaderSize + (index * CycloneWire.QueryReplyEntrySize);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(cursor, 4), id);
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(cursor + 4, 8), fingerprint);
        }

        public static bool TryQueryReplyCount(ReadOnlySpan<byte> payload, out int count)
        {
            count = 0;
            if (payload.Length < CycloneWire.QueryReplyHeaderSize)
            {
                return false;
            }
            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(0, 4));
            if (declared > CycloneWire.MaxQueryItems)
            {
                return false;
            }
            long expected = CycloneWire.QueryReplyHeaderSize + ((long)CycloneWire.QueryReplyEntrySize * declared);
            if (payload.Length != expected)
            {
                return false;
            }
            count = (int)declared;
            return true;
        }

        public static void QueryReplyEntry(ReadOnlySpan<byte> payload, int index, out uint id, out ulong fingerprint)
        {
            int cursor = CycloneWire.QueryReplyHeaderSize + (index * CycloneWire.QueryReplyEntrySize);
            id = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(cursor, 4));
            fingerprint = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(cursor + 4, 8));
        }
    }
}
