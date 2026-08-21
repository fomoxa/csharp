using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Cyclone.Net;
using Cyclone.Net.Transports;

namespace Cyclone.Net.Tests
{
    public static class HandshakeTests
    {
        private const string Group = "handshake";

        private static readonly TimeSpan Zero = TimeSpan.Zero;

        public static void Register()
        {
            TestRegistry.Add(Group, "the client puts its hello on the wire the moment it is built", HelloOnCreation);
            TestRegistry.Add(Group, "a matching schema fingerprint is accepted without reading one entry", FingerprintShortCircuit);
            TestRegistry.Add(Group, "two different schemas whose shared part agrees are accepted", DifferentSchemasAgree);
            TestRegistry.Add(Group, "branch b: same field count, different fingerprint, is refused with 2", SameCountDifferentContent);
            TestRegistry.Add(Group, "branch c: fewer fields on the client, prefix matches, accepted with no query", ClientShorterMatches);
            TestRegistry.Add(Group, "branch c: fewer fields on the client, prefix differs, refused with 2 and no query", ClientShorterDiffers);
            TestRegistry.Add(Group, "branch d: more fields on the client makes the server ask", ClientLongerAsks);
            TestRegistry.Add(Group, "branch d: a server holding zero fields accepts without asking", ServerHasNoFields);
            TestRegistry.Add(Group, "a field appended at the end of the client's message is accepted", ClientAppendedField);
            TestRegistry.Add(Group, "a field dropped from the end of the server's message is accepted", ServerDroppedField);
            TestRegistry.Add(Group, "a wrong fingerprint in the query reply is refused with 2", QueryReplyWrongFingerprint);
            TestRegistry.Add(Group, "a query reply with the wrong number of items is refused with 3", QueryReplyWrongCount);
            TestRegistry.Add(Group, "a query reply in the wrong order is refused with 3", QueryReplyWrongOrder);
            TestRegistry.Add(Group, "a hello carrying another protocol version is refused with 1", WrongVersion);
            TestRegistry.Add(Group, "a hello one byte off its declared size is refused with 3", LengthOffByOne);
            TestRegistry.Add(Group, "an enormous declared message count is refused without overflowing", HugeCount);
            TestRegistry.Add(Group, "the client treats a verdict of 5 as a broken handshake", VerdictOutOfRange);
            TestRegistry.Add(Group, "the client treats a second query as a broken handshake", TwoQueries);
            TestRegistry.Add(Group, "a query naming a message the client never declared is broken", QueryForUnknownMessage);
            TestRegistry.Add(Group, "the client's deadline is not restarted by the query round", DeadlineSurvivesQuery);
            TestRegistry.Add(Group, "the client fails when the handshake deadline passes", ClientDeadline);
            TestRegistry.Add(Group, "a peer that answers probes but never says hello is never timed out", ServerHasNoDeadline);
            TestRegistry.Add(Group, "the application cannot send before the verdict arrives", NoSendBeforeReady);
            TestRegistry.Add(Group, "a DATA frame arriving mid-handshake is dropped, not delivered", DataDuringHandshake);
            TestRegistry.Add(Group, "the client answers a probe even while it is still handshaking", ProbeDuringHandshake);
        }

        private static void HelloOnCreation()
        {
            var schema = Schemas.Of(0xAABBCCDD11223344, Schemas.Message(7, 0x1111, 0x2222));
            var transport = new FakeTransport(TransportKind.Message);
            using var connection = new CycloneConnection(transport, schema, Schemas.Config(), CycloneRole.Client, Zero);

            var frame = transport.TakeOutgoing();
            var payload = PayloadOf(frame);

            Check.Equal(CycloneWire.HelloHeaderSize + CycloneWire.HelloEntrySize, payload.Length, "hello size");
            Check.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4)), "protocol version");
            Check.Equal(
                0xAABBCCDD11223344ul,
                BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(4, 8)),
                "schema fingerprint");
            Check.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(12, 4)), "message count");
            Check.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(16, 4)), "message id");
            Check.Equal((ushort)2, BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(20, 2)), "field count");
            Check.Equal(0x2222ul, BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(22, 8)), "message fingerprint");
        }

        private static void FingerprintShortCircuit()
        {
            var server = Server(Schemas.Of(0xF00D, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 0xF00D, (1, 9, 0xDEAD)));
            var events = server.Connection.Tick(Zero);

            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
            Check.True(Events.Has(events, CycloneEventKind.Ready), "peer became ready");
        }

        private static void DifferentSchemasAgree()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20), Schemas.Message(2, 30)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 2, 20), (3, 1, 77)));
            server.Connection.Tick(Zero);

            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
        }

        private static void SameCountDifferentContent()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 2, 21)));
            var events = server.Connection.Tick(Zero);

            Check.Equal((byte)2, Verdict(server.Transport), "verdict");
            Check.Equal(
                HandshakeFailure.SchemaConflict,
                Events.First(events, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
        }

        private static void ClientShorterMatches()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 1, 10)));
            server.Connection.Tick(Zero);

            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
            Check.Equal(0, server.Transport.Outgoing.Count, "no query was sent");
        }

        private static void ClientShorterDiffers()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 1, 11)));
            server.Connection.Tick(Zero);

            Check.Equal((byte)2, Verdict(server.Transport), "verdict");
            Check.Equal(0, server.Transport.Outgoing.Count, "no query was sent");
        }

        private static void ClientLongerAsks()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 99)));
            var events = server.Connection.Tick(Zero);

            var query = PayloadOf(server.Transport.TakeOutgoing());
            Check.Equal(CycloneWire.QueryVerdictByte, query[0], "query marker byte");
            Check.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(query.AsSpan(1, 4)), "one item asked for");
            Check.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(query.AsSpan(5, 4)), "asked about message 1");
            Check.Equal((ushort)2, BinaryPrimitives.ReadUInt16LittleEndian(query.AsSpan(9, 2)), "asked at index 2");
            Check.False(Events.Has(events, CycloneEventKind.Ready), "a query is not a verdict");
            Check.False(Events.Has(events, CycloneEventKind.HandshakeFailed), "a query never ends the session");

            server.Transport.Deliver(Wire.QueryReply((1, 20)));
            var second = server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict after the reply");
            Check.True(Events.Has(second, CycloneEventKind.Ready), "peer became ready");
        }

        private static void ServerHasNoFields()
        {
            var server = Server(Schemas.Of(1, Schemas.Empty(1, 555)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 99)));
            server.Connection.Tick(Zero);

            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
            Check.Equal(0, server.Transport.Outgoing.Count, "no query was sent");
        }

        private static void ClientAppendedField()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 30)));
            server.Connection.Tick(Zero);
            Check.Equal(CycloneWire.QueryVerdictByte, PayloadOf(server.Transport.TakeOutgoing())[0], "query sent");

            server.Transport.Deliver(Wire.QueryReply((1, 20)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
        }

        private static void ServerDroppedField()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 2, 20)));
            server.Connection.Tick(Zero);
            var query = PayloadOf(server.Transport.TakeOutgoing());
            Check.Equal((ushort)1, BinaryPrimitives.ReadUInt16LittleEndian(query.AsSpan(9, 2)), "asked at index 1");

            server.Transport.Deliver(Wire.QueryReply((1, 10)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)0, Verdict(server.Transport), "verdict");
        }

        private static void QueryReplyWrongFingerprint()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 99)));
            server.Connection.Tick(Zero);
            server.Transport.TakeOutgoing();

            server.Transport.Deliver(Wire.QueryReply((1, 21)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)2, Verdict(server.Transport), "verdict");
        }

        private static void QueryReplyWrongCount()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 99)));
            server.Connection.Tick(Zero);
            server.Transport.TakeOutgoing();

            server.Transport.Deliver(Wire.QueryReply((1, 20), (2, 20)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void QueryReplyWrongOrder()
        {
            var server = Server(
                Schemas.Of(1, Schemas.Message(1, 10, 20), Schemas.Message(2, 40, 50)));
            server.Transport.Deliver(Wire.Hello(2, 999, (1, 3, 99), (2, 3, 98)));
            server.Connection.Tick(Zero);
            server.Transport.TakeOutgoing();

            server.Transport.Deliver(Wire.QueryReply((2, 50), (1, 20)));
            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void WrongVersion()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10)));
            server.Transport.Deliver(Wire.Hello(1, 999, (1, 1, 10)));
            var events = server.Connection.Tick(Zero);

            Check.Equal((byte)1, Verdict(server.Transport), "verdict");
            Check.Equal(
                HandshakeFailure.VersionMismatch,
                Events.First(events, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
        }

        private static void LengthOffByOne()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10)));
            var hello = Wire.Hello(2, 999, (1, 1, 10));
            var shortened = new byte[hello.Length - 1];
            Array.Copy(hello, shortened, shortened.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(
                shortened.AsSpan(1, 4), (uint)(shortened.Length - CycloneWire.HandshakeFrameHeaderSize));

            server.Transport.Deliver(shortened);
            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void HugeCount()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10)));
            var payload = new byte[CycloneWire.HelloHeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 2);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4, 8), 999);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12, 4), uint.MaxValue);

            server.Transport.Deliver(Wire.HandshakeFrame(payload));
            server.Connection.Tick(Zero);
            Check.Equal((byte)3, Verdict(server.Transport), "verdict");
        }

        private static void VerdictOutOfRange()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10)));
            client.Transport.TakeOutgoing();

            client.Transport.Deliver(Wire.Verdict(5));
            var events = client.Connection.Tick(Zero);

            Check.Equal(
                HandshakeFailure.Corrupt,
                Events.First(events, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
        }

        private static void TwoQueries()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10, 20, 30)));
            client.Transport.TakeOutgoing();

            client.Transport.Deliver(Wire.Query((1, 2)));
            var first = client.Connection.Tick(Zero);
            Check.False(Events.Has(first, CycloneEventKind.Ready), "a query is not a verdict");
            Check.False(Events.Has(first, CycloneEventKind.HandshakeFailed), "a query never ends the session");
            var reply = PayloadOf(client.Transport.TakeOutgoing());
            Check.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(reply.AsSpan(0, 4)), "one item answered");
            Check.Equal(20ul, BinaryPrimitives.ReadUInt64LittleEndian(reply.AsSpan(8, 8)), "prefix at index 2");

            client.Transport.Deliver(Wire.Query((1, 1)));
            var second = client.Connection.Tick(Zero);
            Check.Equal(
                HandshakeFailure.Corrupt,
                Events.First(second, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
        }

        private static void QueryForUnknownMessage()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10, 20)));
            client.Transport.TakeOutgoing();

            client.Transport.Deliver(Wire.Query((42, 1)));
            var events = client.Connection.Tick(Zero);
            Check.Equal(
                HandshakeFailure.Corrupt,
                Events.First(events, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
        }

        private static void DeadlineSurvivesQuery()
        {
            var config = Schemas.Config();
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10, 20, 30)), config);
            client.Transport.TakeOutgoing();

            client.Connection.Tick(TimeSpan.FromSeconds(4));
            client.Transport.Deliver(Wire.Query((1, 2)));
            var atFour = client.Connection.Tick(TimeSpan.FromSeconds(4));
            Check.False(Events.Has(atFour, CycloneEventKind.HandshakeFailed), "still handshaking at four seconds");

            var atFive = client.Connection.Tick(TimeSpan.FromSeconds(5));
            Check.Equal(
                HandshakeFailure.Timeout,
                Events.First(atFive, CycloneEventKind.HandshakeFailed).Failure,
                "the query round did not restart the deadline");
        }

        private static void ClientDeadline()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10)));
            client.Transport.TakeOutgoing();

            Check.False(
                Events.Has(client.Connection.Tick(TimeSpan.FromSeconds(4)), CycloneEventKind.HandshakeFailed),
                "nothing has failed at four seconds");
            var events = client.Connection.Tick(TimeSpan.FromSeconds(5));
            Check.Equal(
                HandshakeFailure.Timeout,
                Events.First(events, CycloneEventKind.HandshakeFailed).Failure,
                "failure reason");
            Check.Equal(SessionState.Closed, client.Connection.State, "session state");
        }

        private static void ServerHasNoDeadline()
        {
            var server = Server(Schemas.Of(1, Schemas.Message(1, 10)));
            server.Connection.Tick(Zero);

            var now = Zero;
            for (int round = 0; round < 20; round++)
            {
                now += TimeSpan.FromSeconds(6);
                server.Connection.Tick(now);
                while (server.Transport.Outgoing.Count > 0)
                {
                    Check.Equal((byte)FrameType.Ping, server.Transport.TakeOutgoing()[0], "server probes a silent peer");
                }
                server.Transport.Deliver(Wire.Pong());
                now += TimeSpan.FromSeconds(1);
                server.Connection.Tick(now);
            }

            Check.Equal(SessionState.Handshaking, server.Connection.State, "the peer is still alive");
        }

        private static void NoSendBeforeReady()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10)));
            Check.Equal(SendStatus.NotReady, client.Connection.Send(1, new byte[] { 1 }), "send before ready");

            client.Transport.TakeOutgoing();
            client.Transport.Deliver(Wire.Verdict(0));
            var events = client.Connection.Tick(Zero);
            Check.True(Events.Has(events, CycloneEventKind.Ready), "ready event");
            Check.Equal(SendStatus.Sent, client.Connection.Send(1, new byte[] { 1 }), "send after ready");
        }

        private static void DataDuringHandshake()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10)));
            client.Transport.TakeOutgoing();

            client.Transport.Deliver(Wire.DataFrame(1, new byte[] { 9 }));
            var events = client.Connection.Tick(Zero);
            Check.False(Events.Has(events, CycloneEventKind.Message), "no message reaches the application");
            Check.Equal(SessionState.Handshaking, client.Connection.State, "the session is untouched");
        }

        private static void ProbeDuringHandshake()
        {
            var client = Client(Schemas.Of(1, Schemas.Message(1, 10)));
            client.Transport.TakeOutgoing();

            client.Transport.Deliver(Wire.Ping());
            var events = client.Connection.Tick(Zero);
            Check.Bytes(new byte[] { (byte)FrameType.Pong }, client.Transport.TakeOutgoing(), "the client answers");
            Check.False(Events.Has(events, CycloneEventKind.Ping), "no probe event before ready");
        }

        private readonly struct Side
        {
            public Side(CycloneConnection connection, FakeTransport transport)
            {
                Connection = connection;
                Transport = transport;
            }

            public CycloneConnection Connection { get; }

            public FakeTransport Transport { get; }
        }

        private static Side Server(Schema schema, SessionConfig? config = null)
        {
            var transport = new FakeTransport(TransportKind.Message);
            var connection = new CycloneConnection(
                transport, schema, config ?? Schemas.Config(), CycloneRole.Server, Zero);
            return new Side(connection, transport);
        }

        private static Side Client(Schema schema, SessionConfig? config = null)
        {
            var transport = new FakeTransport(TransportKind.Message);
            var connection = new CycloneConnection(
                transport, schema, config ?? Schemas.Config(), CycloneRole.Client, Zero);
            return new Side(connection, transport);
        }

        private static byte[] PayloadOf(byte[] frame)
        {
            Check.Equal((byte)FrameType.Handshake, frame[0], "frame type");
            int length = (int)BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(1, 4));
            Check.Equal(CycloneWire.HandshakeFrameHeaderSize + length, frame.Length, "handshake frame size");
            var payload = new byte[length];
            Array.Copy(frame, CycloneWire.HandshakeFrameHeaderSize, payload, 0, length);
            return payload;
        }

        private static byte Verdict(FakeTransport transport)
        {
            var payload = PayloadOf(transport.TakeOutgoing());
            Check.Equal(1, payload.Length, "a verdict is one byte");
            return payload[0];
        }
    }
}
