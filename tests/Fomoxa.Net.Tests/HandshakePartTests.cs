using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Tests
{
    public static class HandshakePartTests
    {
        private const string Group = "handshake parts";

        private static readonly TimeSpan Zero = TimeSpan.Zero;

        public static void Register()
        {
            TestRegistry.Add(Group, "a hello of 62 messages leaves as one HANDSHAKE frame, as before", SmallHelloIsOneFrame);
            TestRegistry.Add(Group, "a hello of 63 messages on a packet transport leaves in two parts", LargeHelloIsSplit);
            TestRegistry.Add(Group, "every part is at most 900 bytes and only the last one is short", PartsFitTheBudget);
            TestRegistry.Add(Group, "a stream transport sends a large hello as one HANDSHAKE frame", StreamKeepsOneFrame);
            TestRegistry.Add(Group, "a server assembles a hello sent in parts and accepts it", ServerAssemblesHello);
            TestRegistry.Add(Group, "parts that arrive in reverse order are assembled", PartsOutOfOrder);
            TestRegistry.Add(Group, "a repeated identical part is ignored", DuplicatePartIgnored);
            TestRegistry.Add(Group, "a part that differs from an earlier copy is refused with 3", DuplicatePartDiffers);
            TestRegistry.Add(Group, "a part whose count changes is refused with 3", CountChanges);
            TestRegistry.Add(Group, "a short part before the last one is refused with 3", ShortMiddlePart);
            TestRegistry.Add(Group, "a part count of 1 is refused with 3", CountOfOne);
            TestRegistry.Add(Group, "a part index past the count is refused with 3", IndexPastCount);
            TestRegistry.Add(Group, "a part carrying more than 891 bytes is dropped as a broken packet", OversizedPartDropped);
            TestRegistry.Add(Group, "a HANDSHAKE frame arriving while parts are pending is refused with 3", HandshakeWhilePending);
            TestRegistry.Add(Group, "content assembled past 1 MiB is refused with 3", AssembledTooLarge);
            TestRegistry.Add(Group, "a part after the handshake is dropped and changes nothing", PartAfterReady);
            TestRegistry.Add(Group, "a part on a byte stream is a fatal framing error", PartOnStream);
            TestRegistry.Add(Group, "the client fails on a broken query sent in parts", ClientBrokenParts);
            TestRegistry.Add(Group, "a query and its reply that exceed the budget both travel in parts", QueryRoundInParts);
            TestRegistry.Add(Group, "parts held back by a blocked socket leave later as separate packets", BlockedPartsStaySeparate);
            TestRegistry.Add(Group, "a pending part counts as activity for the server", PartIsActivity);
            TestRegistry.Add(Group, "a hello larger than 64 KiB held by a blocked socket does not hit the ceiling", LargeBlockedHelloWaits);
        }

        private static void SmallHelloIsOneFrame()
        {
            var client = Client(Many(62), TransportKind.Message);

            var frame = client.Transport.TakeOutgoing();
            Check.Equal((byte)FrameType.Handshake, frame[0], "frame type");
            Check.Equal(5 + 16 + (14 * 62), frame.Length, "hello frame size");
            Check.Equal(0, client.Transport.Outgoing.Count, "nothing else was sent");
        }

        private static void LargeHelloIsSplit()
        {
            var schema = Many(63);
            var client = Client(schema, TransportKind.Message);

            var first = client.Transport.TakeOutgoing();
            var second = client.Transport.TakeOutgoing();
            Check.Equal(0, client.Transport.Outgoing.Count, "two parts and nothing else");
            Check.Equal((byte)FrameType.HandshakePart, first[0], "first frame type");
            Check.Equal((byte)FrameType.HandshakePart, second[0], "second frame type");
            Check.Equal((ushort)0, BinaryPrimitives.ReadUInt16LittleEndian(first.AsSpan(1, 2)), "first index");
            Check.Equal((ushort)1, BinaryPrimitives.ReadUInt16LittleEndian(second.AsSpan(1, 2)), "second index");
            Check.Equal((ushort)2, BinaryPrimitives.ReadUInt16LittleEndian(first.AsSpan(3, 2)), "count in the first part");
            Check.Equal((ushort)2, BinaryPrimitives.ReadUInt16LittleEndian(second.AsSpan(3, 2)), "count in the second part");
            Check.Equal(891u, BinaryPrimitives.ReadUInt32LittleEndian(first.AsSpan(5, 4)), "first length");
            Check.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(second.AsSpan(5, 4)), "second length");

            var joined = new byte[891 + 7];
            Array.Copy(first, 9, joined, 0, 891);
            Array.Copy(second, 9, joined, 891, 7);
            var expected = HelloPayload(Single(Client(schema, TransportKind.Stream).Transport));
            Check.Bytes(expected, joined, "the parts carry the hello unchanged");
        }

        private static void PartsFitTheBudget()
        {
            var client = Client(Many(500), TransportKind.Message);
            var parts = Drain(client.Transport);
            int content = 16 + (14 * 500);

            Check.Equal((content + 890) / 891, parts.Count, "number of parts");
            for (int index = 0; index < parts.Count; index++)
            {
                Check.True(parts[index].Length <= 900, $"part {index} fits 900 bytes");
                bool last = index == parts.Count - 1;
                Check.True(last || parts[index].Length == 900, $"part {index} is full unless it is the last");
            }
        }

        private static void StreamKeepsOneFrame()
        {
            var client = Client(Many(200), TransportKind.Stream);

            var frame = client.Transport.TakeOutgoing();
            Check.Equal((byte)FrameType.Handshake, frame[0], "frame type");
            Check.Equal(5 + 16 + (14 * 200), frame.Length, "hello frame size");
            Check.Equal(0, client.Transport.Outgoing.Count, "one frame only");
        }

        private static void ServerAssemblesHello()
        {
            var schema = Many(200);
            var client = Client(schema, TransportKind.Message);
            var server = Server(schema);

            Pipe.Pump(client.Transport, server.Transport);
            var serverEvents = server.Connection.Tick(Zero);
            Check.True(Events.Has(serverEvents, FomoxaEventKind.Ready), "the server accepted");

            Pipe.Pump(server.Transport, client.Transport);
            var clientEvents = client.Connection.Tick(Zero);
            Check.True(Events.Has(clientEvents, FomoxaEventKind.Ready), "the client got the verdict");
        }

        private static void PartsOutOfOrder()
        {
            var schema = Many(200);
            var parts = Drain(Client(schema, TransportKind.Message).Transport);
            parts.Reverse();
            var server = Server(schema);
            foreach (var part in parts)
            {
                server.Transport.Deliver(part);
            }

            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
        }

        private static void DuplicatePartIgnored()
        {
            var schema = Many(200);
            var parts = Drain(Client(schema, TransportKind.Message).Transport);
            var server = Server(schema);
            server.Transport.Deliver(parts[0]);
            server.Transport.Deliver(parts[0]);
            for (int index = 1; index < parts.Count; index++)
            {
                server.Transport.Deliver(parts[index]);
            }

            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
        }

        private static void DuplicatePartDiffers()
        {
            var schema = Many(200);
            var parts = Drain(Client(schema, TransportKind.Message).Transport);
            var altered = (byte[])parts[0].Clone();
            altered[20] ^= 0xFF;
            var server = Server(schema);
            server.Transport.Deliver(parts[0]);
            server.Transport.Deliver(altered);

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void CountChanges()
        {
            var server = Server(Many(200));
            server.Transport.Deliver(Part(0, 3, new byte[891]));
            server.Transport.Deliver(Part(1, 4, new byte[891]));

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void ShortMiddlePart()
        {
            var server = Server(Many(200));
            server.Transport.Deliver(Part(0, 3, new byte[890]));

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void CountOfOne()
        {
            var schema = Many(62);
            var hello = HelloPayload(Single(Client(schema, TransportKind.Stream).Transport));
            var server = Server(schema);
            server.Transport.Deliver(Part(0, 1, hello));

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void IndexPastCount()
        {
            var server = Server(Many(200));
            server.Transport.Deliver(Part(2, 2, new byte[891]));

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void OversizedPartDropped()
        {
            var schema = Many(200);
            var server = Server(schema);
            server.Transport.Deliver(Part(0, 2, new byte[892]));
            var events = server.Connection.Tick(Zero);

            Check.Equal(0, server.Transport.Outgoing.Count, "no verdict was sent");
            Check.False(Events.Has(events, FomoxaEventKind.HandshakeFailed), "the handshake goes on");

            foreach (var part in Drain(Client(schema, TransportKind.Message).Transport))
            {
                server.Transport.Deliver(part);
            }
            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "a correct hello is still accepted");
        }

        private static void HandshakeWhilePending()
        {
            var schema = Many(200);
            var parts = Drain(Client(schema, TransportKind.Message).Transport);
            var server = Server(schema);
            server.Transport.Deliver(parts[0]);
            server.Transport.Deliver(Wire.Hello(2, schema.Fingerprint));

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void AssembledTooLarge()
        {
            const int entries = 74_900;
            var config = Schemas.Config();
            config.MaxFramesPerTick = 2048;
            var schema = Many(1);
            var server = Server(schema, config);
            var hello = new byte[FomoxaWire.HelloHeaderSize + (FomoxaWire.HelloEntrySize * entries)];
            BinaryPrimitives.WriteUInt32LittleEndian(hello.AsSpan(0, 4), FomoxaWire.ProtocolVersion);
            BinaryPrimitives.WriteUInt64LittleEndian(hello.AsSpan(4, 8), schema.Fingerprint);
            BinaryPrimitives.WriteUInt32LittleEndian(hello.AsSpan(12, 4), entries);
            Check.True(hello.Length > FomoxaWire.MaxHandshakePayload, "the hello is past 1 MiB");

            int count = (hello.Length + 890) / 891;
            for (int index = 0; index < count; index++)
            {
                int offset = index * 891;
                server.Transport.Deliver(Part(index, count, hello.AsSpan(offset, Math.Min(891, hello.Length - offset)).ToArray()));
            }

            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void PartAfterReady()
        {
            var schema = Many(1);
            var server = Server(schema);
            server.Transport.Deliver(Wire.Hello(2, schema.Fingerprint, (1, 1, 1)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");

            var config = Schemas.Config();
            var almost = config.HeartbeatInterval - TimeSpan.FromMilliseconds(1);
            server.Transport.Deliver(Part(0, 2, new byte[891]));
            var events = server.Connection.Tick(almost);
            Check.Equal(0, events.Count, "no event");
            Check.Equal(0, server.Transport.Outgoing.Count, "nothing sent");
            Check.True(server.Connection.IsReady, "still ready");

            server.Connection.Tick(config.HeartbeatInterval);
            Check.Equal((byte)FrameType.Ping, server.Transport.TakeOutgoing()[0], "the dropped part was not activity");
        }

        private static void PartOnStream()
        {
            var transport = new FakeTransport(TransportKind.Stream);
            using var server = new FomoxaConnection(transport, Many(1), Schemas.Config(), FomoxaRole.Server, Zero);
            transport.Deliver(Part(0, 2, new byte[891]));

            var events = server.Tick(Zero);
            Check.True(Events.Has(events, FomoxaEventKind.Disconnected), "the session ended");
            Check.Equal(
                DisconnectReason.TransportError,
                Events.First(events, FomoxaEventKind.Disconnected).Reason,
                "reason");
        }

        private static void ClientBrokenParts()
        {
            var client = Client(Many(1), TransportKind.Message);
            client.Transport.TakeOutgoing();
            client.Transport.Deliver(Part(0, 2, new byte[891]));
            client.Transport.Deliver(Part(1, 2, new byte[0]));

            var events = client.Connection.Tick(Zero);
            Check.True(Events.Has(events, FomoxaEventKind.HandshakeFailed), "the client failed");
            Check.Equal(
                HandshakeFailure.Corrupt,
                Events.First(events, FomoxaEventKind.HandshakeFailed).Failure,
                "failure");
        }

        private static void QueryRoundInParts()
        {
            const int messages = 160;
            var clientMessages = new MessageSchema[messages];
            var serverMessages = new MessageSchema[messages];
            for (uint id = 1; id <= messages; id++)
            {
                clientMessages[id - 1] = Schemas.Message(id, id * 10, (id * 10) + 1);
                serverMessages[id - 1] = Schemas.Message(id, id * 10);
            }
            var client = Client(Schemas.Of(1, clientMessages), TransportKind.Message);
            var server = Server(Schemas.Of(2, serverMessages));

            Pipe.Pump(client.Transport, server.Transport);
            server.Connection.Tick(Zero);
            var query = Drain(server.Transport);
            Check.Equal(2, query.Count, "the query leaves in two parts");
            Check.Equal((byte)FrameType.HandshakePart, query[0][0], "query part type");

            foreach (var part in query)
            {
                client.Transport.Deliver(part);
            }
            client.Connection.Tick(Zero);
            var reply = Drain(client.Transport);
            Check.Equal(3, reply.Count, "the reply leaves in three parts");

            foreach (var part in reply)
            {
                server.Transport.Deliver(part);
            }
            var serverEvents = server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
            Check.True(Events.Has(serverEvents, FomoxaEventKind.Ready), "the server is ready");
        }

        private static void BlockedPartsStaySeparate()
        {
            var transport = new FakeTransport(TransportKind.Message) { BlockSends = true };
            using var client = new FomoxaConnection(transport, Many(200), Schemas.Config(), FomoxaRole.Client, Zero);
            Check.Equal(0, transport.Outgoing.Count, "nothing left while blocked");
            Check.True(client.IsCongested, "the parts are waiting");

            transport.BlockSends = false;
            client.Tick(Zero);
            var parts = Drain(transport);
            Check.Equal((16 + (14 * 200) + 890) / 891, parts.Count, "one packet per part");
            foreach (var part in parts)
            {
                Check.Equal((byte)FrameType.HandshakePart, part[0], "part type");
                int length = (int)BinaryPrimitives.ReadUInt32LittleEndian(part.AsSpan(5, 4));
                Check.Equal(9 + length, part.Length, "each packet holds exactly one part");
            }
            Check.False(client.IsCongested, "nothing is waiting any more");
        }

        private static void LargeBlockedHelloWaits()
        {
            var transport = new FakeTransport(TransportKind.Message) { BlockSends = true };
            using var client = new FomoxaConnection(transport, Many(5000), Schemas.Config(), FomoxaRole.Client, Zero);
            var events = client.Tick(Zero);
            Check.False(Events.Has(events, FomoxaEventKind.Disconnected), "the session is still open");
            Check.True((16 + (14 * 5000)) > 64 * 1024, "the hello is larger than the control ceiling");

            transport.BlockSends = false;
            client.Tick(Zero);
            Check.Equal((16 + (14 * 5000) + 890) / 891, transport.Outgoing.Count, "every part left");
        }

        private static void PartIsActivity()
        {
            var config = Schemas.Config();
            var server = Server(Many(200), config);
            var parts = Drain(Client(Many(200), TransportKind.Message).Transport);

            var almost = config.HandshakeTimeout - TimeSpan.FromMilliseconds(1);
            server.Transport.Deliver(parts[0]);
            server.Connection.Tick(almost);
            server.Connection.Tick(almost + almost);
            Check.Equal(0, server.Transport.Outgoing.Count, "no probe while parts keep the peer active");
        }

        private static Schema Many(int count)
        {
            var messages = new MessageSchema[count];
            for (uint id = 1; id <= count; id++)
            {
                messages[id - 1] = Schemas.Message(id, id);
            }
            return Schemas.Of(0xC0FFEE00UL + (ulong)count, messages);
        }

        private static byte[] Part(int index, int count, byte[] content)
        {
            var frame = new byte[FomoxaWire.HandshakePartHeaderSize + content.Length];
            frame[0] = (byte)FrameType.HandshakePart;
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(1, 2), (ushort)index);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(3, 2), (ushort)count);
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(5, 4), (uint)content.Length);
            content.CopyTo(frame.AsSpan(FomoxaWire.HandshakePartHeaderSize));
            return frame;
        }

        private static List<byte[]> Drain(FakeTransport transport)
        {
            var frames = new List<byte[]>();
            while (transport.Outgoing.Count > 0)
            {
                frames.Add(transport.Outgoing.Dequeue());
            }
            return frames;
        }

        private static byte[] Single(FakeTransport transport)
        {
            var frame = transport.TakeOutgoing();
            Check.Equal(0, transport.Outgoing.Count, "one frame only");
            return frame;
        }

        private static byte[] HelloPayload(byte[] frame)
        {
            Check.Equal((byte)FrameType.Handshake, frame[0], "frame type");
            var payload = new byte[frame.Length - FomoxaWire.HandshakeFrameHeaderSize];
            Array.Copy(frame, FomoxaWire.HandshakeFrameHeaderSize, payload, 0, payload.Length);
            return payload;
        }

        private static byte Verdict(FakeTransport transport)
        {
            var frame = transport.TakeOutgoing();
            Check.Equal((byte)FrameType.Handshake, frame[0], "verdict frame type");
            Check.Equal(FomoxaWire.HandshakeFrameHeaderSize + 1, frame.Length, "a verdict is one byte");
            return frame[FomoxaWire.HandshakeFrameHeaderSize];
        }

        private static Side Server(Schema schema, SessionConfig? config = null)
        {
            var transport = new FakeTransport(TransportKind.Message);
            var connection = new FomoxaConnection(
                transport, schema, config ?? Schemas.Config(), FomoxaRole.Server, Zero);
            return new Side(connection, transport);
        }

        private static Side Client(Schema schema, TransportKind kind)
        {
            var transport = new FakeTransport(kind);
            var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, Zero);
            return new Side(connection, transport);
        }

        private sealed class Side
        {
            public Side(FomoxaConnection connection, FakeTransport transport)
            {
                Connection = connection;
                Transport = transport;
            }

            public FomoxaConnection Connection { get; }

            public FakeTransport Transport { get; }
        }
    }
}
