using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Cyclone.Net;
using Cyclone.Net.Transports;

namespace Cyclone.Net.Tests
{
    public sealed class FakeTransport : ITransport
    {
        public FakeTransport(TransportKind kind)
        {
            Kind = kind;
        }

        public TransportKind Kind { get; }

        public Queue<byte[]> Outgoing { get; } = new Queue<byte[]>();

        public Queue<byte[]> Incoming { get; } = new Queue<byte[]>();

        public bool BlockSends { get; set; }

        public int SendCeiling { get; set; } = int.MaxValue;

        public bool ReportClosed { get; set; }

        public bool ReportError { get; set; }

        public int SendAttempts { get; private set; }

        public bool GracefullyClosed { get; private set; }

        public bool Released { get; private set; }

        public SendOutcome Send(ReadOnlySpan<byte> bytes)
        {
            SendAttempts++;
            if (ReportClosed)
            {
                return SendOutcome.Closed;
            }
            if (ReportError)
            {
                return SendOutcome.Error;
            }
            if (bytes.Length > SendCeiling)
            {
                return SendOutcome.TooLarge;
            }
            if (BlockSends)
            {
                return SendOutcome.WouldBlock;
            }
            Outgoing.Enqueue(bytes.ToArray());
            return SendOutcome.Ok;
        }

        public ReceiveOutcome Receive(Span<byte> buffer)
        {
            if (Incoming.Count == 0)
            {
                if (ReportClosed)
                {
                    return ReceiveOutcome.Closed;
                }
                if (ReportError)
                {
                    return ReceiveOutcome.Error;
                }
                return ReceiveOutcome.WouldBlock;
            }

            var packet = Incoming.Peek();
            if (packet.Length > buffer.Length)
            {
                return ReceiveOutcome.NeedCapacity(packet.Length);
            }
            packet.CopyTo(buffer);
            Incoming.Dequeue();
            return ReceiveOutcome.Ok(packet.Length);
        }

        public void CloseGracefully() => GracefullyClosed = true;

        public void Dispose() => Released = true;

        public byte[] TakeOutgoing()
        {
            if (Outgoing.Count == 0)
            {
                throw new AssertionException("nothing was written to the transport");
            }
            return Outgoing.Dequeue();
        }

        public void Deliver(byte[] bytes) => Incoming.Enqueue(bytes);
    }

    public static class Pipe
    {
        public static void Pump(FakeTransport from, FakeTransport to)
        {
            while (from.Outgoing.Count > 0)
            {
                to.Incoming.Enqueue(from.Outgoing.Dequeue());
            }
        }

        public static void PumpFragmented(FakeTransport from, FakeTransport to)
        {
            while (from.Outgoing.Count > 0)
            {
                var block = from.Outgoing.Dequeue();
                foreach (var single in block)
                {
                    to.Incoming.Enqueue(new[] { single });
                }
            }
        }

        public static void Exchange(FakeTransport left, FakeTransport right)
        {
            Pump(left, right);
            Pump(right, left);
        }

        public static void ExchangeFragmented(FakeTransport left, FakeTransport right)
        {
            PumpFragmented(left, right);
            PumpFragmented(right, left);
        }
    }

    public sealed class FakeListener : IListenerTransport
    {
        public Queue<ITransport> Waiting { get; } = new Queue<ITransport>();

        public bool Released { get; private set; }

        public AcceptOutcome Accept() =>
            Waiting.Count > 0 ? AcceptOutcome.Accepted(Waiting.Dequeue()) : AcceptOutcome.Pending;

        public void Dispose() => Released = true;
    }

    public static class Wire
    {
        public static byte[] HandshakeFrame(byte[] payload)
        {
            var frame = new byte[CycloneWire.HandshakeFrameHeaderSize + payload.Length];
            frame[0] = (byte)FrameType.Handshake;
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(1, 4), (uint)payload.Length);
            payload.CopyTo(frame.AsSpan(CycloneWire.HandshakeFrameHeaderSize));
            return frame;
        }

        public static byte[] DataFrame(uint messageId, byte[] payload)
        {
            var frame = new byte[CycloneWire.DataFrameHeaderSize + payload.Length];
            frame[0] = (byte)FrameType.Data;
            frame[1] = CycloneWire.DataMagicFirst;
            frame[2] = CycloneWire.DataMagicSecond;
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(3, 4), messageId);
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(7, 4), (uint)payload.Length);
            payload.CopyTo(frame.AsSpan(CycloneWire.DataFrameHeaderSize));
            return frame;
        }

        public static byte[] Ping() => new byte[] { (byte)FrameType.Ping };

        public static byte[] Pong() => new byte[] { (byte)FrameType.Pong };

        public static byte[] Verdict(byte value) => HandshakeFrame(new byte[] { value });

        public static byte[] Hello(uint version, ulong schemaFingerprint, params (uint Id, ushort Fields, ulong Fingerprint)[] entries)
        {
            var payload = new byte[CycloneWire.HelloHeaderSize + (CycloneWire.HelloEntrySize * entries.Length)];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), version);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4, 8), schemaFingerprint);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12, 4), (uint)entries.Length);
            int cursor = CycloneWire.HelloHeaderSize;
            foreach (var entry in entries)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(cursor, 4), entry.Id);
                BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(cursor + 4, 2), entry.Fields);
                BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(cursor + 6, 8), entry.Fingerprint);
                cursor += CycloneWire.HelloEntrySize;
            }
            return HandshakeFrame(payload);
        }

        public static byte[] QueryReply(params (uint Id, ulong Fingerprint)[] entries)
        {
            var payload = new byte[CycloneWire.QueryReplyHeaderSize + (CycloneWire.QueryReplyEntrySize * entries.Length)];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)entries.Length);
            int cursor = CycloneWire.QueryReplyHeaderSize;
            foreach (var entry in entries)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(cursor, 4), entry.Id);
                BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(cursor + 4, 8), entry.Fingerprint);
                cursor += CycloneWire.QueryReplyEntrySize;
            }
            return HandshakeFrame(payload);
        }

        public static byte[] Query(params (uint Id, ushort Fields)[] entries)
        {
            var payload = new byte[CycloneWire.QueryHeaderSize + (CycloneWire.QueryEntrySize * entries.Length)];
            payload[0] = CycloneWire.QueryVerdictByte;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(1, 4), (uint)entries.Length);
            int cursor = CycloneWire.QueryHeaderSize;
            foreach (var entry in entries)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(cursor, 4), entry.Id);
                BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(cursor + 4, 2), entry.Fields);
                cursor += CycloneWire.QueryEntrySize;
            }
            return HandshakeFrame(payload);
        }
    }

    public static class Schemas
    {
        public static MessageSchema Message(uint id, params ulong[] prefixes)
        {
            ulong fingerprint = prefixes.Length == 0 ? id : prefixes[prefixes.Length - 1];
            return new MessageSchema(id, fingerprint, prefixes);
        }

        public static MessageSchema Empty(uint id, ulong fingerprint) =>
            new MessageSchema(id, fingerprint, Array.Empty<ulong>());

        public static Schema Of(ulong fingerprint, params MessageSchema[] messages) =>
            new Schema(fingerprint, messages);

        public static SessionConfig Config() => new SessionConfig
        {
            HandshakeTimeout = TimeSpan.FromSeconds(5),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
            HeartbeatTimeout = TimeSpan.FromSeconds(15),
            MaxFramesPerTick = 8,
        };
    }

    public static class Events
    {
        public static bool Has(IReadOnlyList<CycloneEvent> events, CycloneEventKind kind)
        {
            foreach (var item in events)
            {
                if (item.Kind == kind)
                {
                    return true;
                }
            }
            return false;
        }

        public static CycloneEvent First(IReadOnlyList<CycloneEvent> events, CycloneEventKind kind)
        {
            foreach (var item in events)
            {
                if (item.Kind == kind)
                {
                    return item;
                }
            }
            throw new AssertionException($"no {kind} event was raised");
        }

        public static int Count(IReadOnlyList<CycloneEvent> events, CycloneEventKind kind)
        {
            int total = 0;
            foreach (var item in events)
            {
                if (item.Kind == kind)
                {
                    total++;
                }
            }
            return total;
        }
    }
}
