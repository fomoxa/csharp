using System;
using Cyclone.Net;
using Cyclone.Net.Framing;
using Cyclone.Net.Transports;

namespace Cyclone.Net.Tests
{
    public static class FrameTests
    {
        private const string Group = "wire format";

        public static void Register()
        {
            TestRegistry.Add(Group, "a DATA frame is 11 bytes of header then payload", DataFrameLayout);
            TestRegistry.Add(Group, "a HANDSHAKE frame is 5 bytes of header then payload", HandshakeFrameLayout);
            TestRegistry.Add(Group, "PING and PONG are one byte and nothing else", ProbeFrameLayout);
            TestRegistry.Add(Group, "a byte stream fed one byte at a time still yields the frames", ByteAtATime);
            TestRegistry.Add(Group, "frames back to back in one push are all split out", BackToBack);
            TestRegistry.Add(Group, "an unknown type byte poisons the stream decoder for good", UnknownTypePoisons);
            TestRegistry.Add(Group, "a DATA frame without the CY marker is corrupt", MissingMarker);
            TestRegistry.Add(Group, "16 MiB of payload is allowed, one byte more is not", DataCeiling);
            TestRegistry.Add(Group, "1 MiB of handshake content is allowed, one byte more is not", HandshakeCeiling);
            TestRegistry.Add(Group, "a packet holding less than one frame is dropped", ShortPacketDropped);
            TestRegistry.Add(Group, "a packet with bytes left over after one frame is dropped", LongPacketDropped);
            TestRegistry.Add(Group, "an unknown type byte only costs one packet on a datagram link", PacketViolationIsLocal);
        }

        private static void DataFrameLayout()
        {
            var payload = new byte[] { 0xAA, 0xBB };
            var frame = new byte[FrameLayout.DataFrameSize(payload.Length)];
            FrameLayout.WriteDataFrame(frame, 42, payload);

            Check.Bytes(
                new byte[] { 0x00, 0x43, 0x59, 0x2A, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB },
                frame,
                "DATA frame bytes");

            var scan = FrameLayout.Scan(frame, out var type, out uint id, out int header, out int length);
            Check.Equal(FrameScan.Ok, scan, "scan status");
            Check.Equal(FrameType.Data, type, "frame type");
            Check.Equal(42u, id, "message id");
            Check.Equal(11, header, "header size");
            Check.Equal(2, length, "payload length");
        }

        private static void HandshakeFrameLayout()
        {
            var frame = new byte[FrameLayout.HandshakeFrameSize(1)];
            FrameLayout.WriteHandshakeHeader(frame, 1);
            frame[5] = 0;

            Check.Bytes(new byte[] { 0x03, 0x01, 0x00, 0x00, 0x00, 0x00 }, frame, "HANDSHAKE frame bytes");

            var scan = FrameLayout.Scan(frame, out var type, out _, out int header, out int length);
            Check.Equal(FrameScan.Ok, scan, "scan status");
            Check.Equal(FrameType.Handshake, type, "frame type");
            Check.Equal(5, header, "header size");
            Check.Equal(1, length, "payload length");
        }

        private static void ProbeFrameLayout()
        {
            var ping = new byte[1];
            FrameLayout.WritePing(ping);
            Check.Bytes(new byte[] { 0x01 }, ping, "PING bytes");

            var pong = new byte[1];
            FrameLayout.WritePong(pong);
            Check.Bytes(new byte[] { 0x02 }, pong, "PONG bytes");

            Check.Equal(
                FrameScan.Ok,
                FrameLayout.Scan(ping, out var pingType, out _, out int pingHeader, out int pingLength),
                "PING scan");
            Check.Equal(FrameType.Ping, pingType, "PING type");
            Check.Equal(1, pingHeader, "PING header size");
            Check.Equal(0, pingLength, "PING payload length");

            Check.Equal(
                FrameScan.Ok,
                FrameLayout.Scan(pong, out var pongType, out _, out _, out _),
                "PONG scan");
            Check.Equal(FrameType.Pong, pongType, "PONG type");
        }

        private static void ByteAtATime()
        {
            var frame = Wire.DataFrame(7, new byte[] { 1, 2, 3, 4, 5 });
            var decoder = new StreamFrameDecoder();

            for (int index = 0; index < frame.Length - 1; index++)
            {
                decoder.Push(new ReadOnlySpan<byte>(frame, index, 1));
                Check.Equal(
                    FrameScan.Incomplete,
                    decoder.TryRead(out _, out _, out _, out _),
                    $"no frame after {index + 1} of {frame.Length} bytes");
            }

            decoder.Push(new ReadOnlySpan<byte>(frame, frame.Length - 1, 1));
            Check.Equal(
                FrameScan.Ok,
                decoder.TryRead(out var type, out uint id, out int offset, out int length),
                "frame after the last byte");
            Check.Equal(FrameType.Data, type, "frame type");
            Check.Equal(7u, id, "message id");
            Check.Bytes(new byte[] { 1, 2, 3, 4, 5 }, decoder.Buffer.AsSpan(offset, length), "payload");
        }

        private static void BackToBack()
        {
            var decoder = new StreamFrameDecoder();
            var first = Wire.DataFrame(1, new byte[] { 0xEE });
            var second = Wire.Ping();
            var third = Wire.HandshakeFrame(new byte[] { 0 });

            var joined = new byte[first.Length + second.Length + third.Length];
            first.CopyTo(joined, 0);
            second.CopyTo(joined, first.Length);
            third.CopyTo(joined, first.Length + second.Length);
            decoder.Push(joined);

            Check.Equal(FrameScan.Ok, decoder.TryRead(out var one, out _, out _, out _), "first frame");
            Check.Equal(FrameType.Data, one, "first frame type");
            Check.Equal(FrameScan.Ok, decoder.TryRead(out var two, out _, out _, out _), "second frame");
            Check.Equal(FrameType.Ping, two, "second frame type");
            Check.Equal(FrameScan.Ok, decoder.TryRead(out var three, out _, out _, out _), "third frame");
            Check.Equal(FrameType.Handshake, three, "third frame type");
            Check.Equal(FrameScan.Incomplete, decoder.TryRead(out _, out _, out _, out _), "nothing left");
        }

        private static void UnknownTypePoisons()
        {
            var decoder = new StreamFrameDecoder();
            decoder.Push(new byte[] { 0x09 });
            Check.Equal(FrameScan.Corrupt, decoder.TryRead(out _, out _, out _, out _), "unknown type byte");
            Check.True(decoder.IsPoisoned, "decoder is poisoned");

            decoder.Push(Wire.Ping());
            Check.Equal(
                FrameScan.Corrupt,
                decoder.TryRead(out _, out _, out _, out _),
                "a poisoned decoder never reads again");
        }

        private static void MissingMarker()
        {
            var frame = Wire.DataFrame(1, new byte[] { 0x00 });
            frame[2] = 0x58;
            var decoder = new StreamFrameDecoder();
            decoder.Push(frame);
            Check.Equal(FrameScan.Corrupt, decoder.TryRead(out _, out _, out _, out _), "wrong marker byte");
        }

        private static void DataCeiling()
        {
            var atCeiling = new byte[CycloneWire.DataFrameHeaderSize];
            atCeiling[0] = (byte)FrameType.Data;
            atCeiling[1] = CycloneWire.DataMagicFirst;
            atCeiling[2] = CycloneWire.DataMagicSecond;
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                atCeiling.AsSpan(7, 4), CycloneWire.MaxMessagePayload);
            Check.Equal(
                FrameScan.Incomplete,
                FrameLayout.Scan(atCeiling, out _, out _, out _, out _),
                "16 MiB is a length the decoder accepts and waits for");

            var pastCeiling = (byte[])atCeiling.Clone();
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                pastCeiling.AsSpan(7, 4), CycloneWire.MaxMessagePayload + 1u);
            Check.Equal(
                FrameScan.Corrupt,
                FrameLayout.Scan(pastCeiling, out _, out _, out _, out _),
                "16 MiB + 1 is rejected");
        }

        private static void HandshakeCeiling()
        {
            var atCeiling = new byte[CycloneWire.HandshakeFrameHeaderSize];
            atCeiling[0] = (byte)FrameType.Handshake;
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                atCeiling.AsSpan(1, 4), CycloneWire.MaxHandshakePayload);
            Check.Equal(
                FrameScan.Incomplete,
                FrameLayout.Scan(atCeiling, out _, out _, out _, out _),
                "1 MiB is a length the decoder accepts and waits for");

            var pastCeiling = (byte[])atCeiling.Clone();
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                pastCeiling.AsSpan(1, 4), CycloneWire.MaxHandshakePayload + 1u);
            Check.Equal(
                FrameScan.Corrupt,
                FrameLayout.Scan(pastCeiling, out _, out _, out _, out _),
                "1 MiB + 1 is rejected");
        }

        private static void ShortPacketDropped()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var full = Wire.DataFrame(1, new byte[] { 1, 2, 3 });
            var truncated = new byte[full.Length - 1];
            Array.Copy(full, truncated, truncated.Length);
            transport.Deliver(truncated);
            transport.Deliver(Wire.Ping());

            var source = new MessageFrameSource(transport);
            var frame = default(SourcedFrame);
            Check.Equal(FrameSourceStatus.Frame, source.Next(ref frame), "the next good packet still arrives");
            Check.Equal(FrameType.Ping, frame.Type, "the truncated packet was skipped");
        }

        private static void LongPacketDropped()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var full = Wire.DataFrame(1, new byte[] { 1, 2, 3 });
            var padded = new byte[full.Length + 1];
            full.CopyTo(padded, 0);
            transport.Deliver(padded);
            transport.Deliver(Wire.Pong());

            var source = new MessageFrameSource(transport);
            var frame = default(SourcedFrame);
            Check.Equal(FrameSourceStatus.Frame, source.Next(ref frame), "the next good packet still arrives");
            Check.Equal(FrameType.Pong, frame.Type, "the padded packet was skipped");
        }

        private static void PacketViolationIsLocal()
        {
            var transport = new FakeTransport(TransportKind.Message);
            transport.Deliver(new byte[] { 0x09, 0x01 });
            transport.Deliver(Wire.Ping());

            var source = new MessageFrameSource(transport);
            var frame = default(SourcedFrame);
            Check.Equal(FrameSourceStatus.Frame, source.Next(ref frame), "a bad packet does not end the session");
            Check.Equal(FrameType.Ping, frame.Type, "only the bad packet was lost");
        }
    }
}
