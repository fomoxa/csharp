using System;
using System.Collections.Generic;
using Cyclone.Net;
using Cyclone.Net.Transports;

namespace Cyclone.Net.Tests
{
    public static class InteropTests
    {
        private const string Group = "two peers end to end";

        public static void Register()
        {
            TestRegistry.Add(Group, "a packet link carries a handshake and traffic both ways", PacketLink);
            TestRegistry.Add(Group, "a byte-stream link delivered one byte at a time works the same", FragmentedStreamLink);
            TestRegistry.Add(Group, "schemas that differ at the end still complete, via the query round", QueryRoundEndToEnd);
            TestRegistry.Add(Group, "a schema conflict closes both sides", ConflictEndToEnd);
            TestRegistry.Add(Group, "the server tracks peers, delivers messages and drops closed ones", ServerLifecycle);
            TestRegistry.Add(Group, "heartbeat frames flow without either side declaring the other dead", HeartbeatEndToEnd);
        }

        private static void PacketLink()
        {
            var link = Link(TransportKind.Message, SharedSchema(), SharedSchema());
            link.Settle(TimeSpan.Zero);

            Check.Equal(SessionState.Ready, link.Client.State, "client is ready");
            Check.Equal(SessionState.Ready, link.Server.State, "server peer is ready");

            Check.Equal(SendStatus.Sent, link.Client.Send(11, new byte[] { 1, 2, 3 }), "client sends");
            Check.Equal(SendStatus.Sent, link.Server.Send(22, new byte[] { 9 }), "server sends");
            link.Settle(TimeSpan.Zero);

            var atServer = Events.First(link.ServerEvents, CycloneEventKind.Message);
            Check.Equal(11u, atServer.MessageId, "message id at the server");
            Check.Bytes(new byte[] { 1, 2, 3 }, atServer.Payload.Span, "payload at the server");

            var atClient = Events.First(link.ClientEvents, CycloneEventKind.Message);
            Check.Equal(22u, atClient.MessageId, "message id at the client");
            Check.Bytes(new byte[] { 9 }, atClient.Payload.Span, "payload at the client");
        }

        private static void FragmentedStreamLink()
        {
            var link = Link(TransportKind.Stream, SharedSchema(), SharedSchema()) with { Fragment = true };
            link.Settle(TimeSpan.Zero);

            Check.Equal(SessionState.Ready, link.Client.State, "client is ready");
            Check.Equal(SessionState.Ready, link.Server.State, "server peer is ready");

            var payload = new byte[300];
            for (int index = 0; index < payload.Length; index++)
            {
                payload[index] = (byte)index;
            }
            Check.Equal(SendStatus.Sent, link.Client.Send(7, payload), "client sends");
            link.Settle(TimeSpan.Zero);

            var received = Events.First(link.ServerEvents, CycloneEventKind.Message);
            Check.Equal(7u, received.MessageId, "message id");
            Check.Bytes(payload, received.Payload.Span, "payload reassembled from single bytes");
        }

        private static void QueryRoundEndToEnd()
        {
            var clientSchema = Schemas.Of(0xC1, Schemas.Message(1, 10, 20, 30));
            var serverSchema = Schemas.Of(0x5E, Schemas.Message(1, 10, 20));
            var link = Link(TransportKind.Message, clientSchema, serverSchema);
            link.Settle(TimeSpan.Zero);

            Check.Equal(SessionState.Ready, link.Client.State, "client is ready");
            Check.Equal(SessionState.Ready, link.Server.State, "server peer is ready");
        }

        private static void ConflictEndToEnd()
        {
            var clientSchema = Schemas.Of(0xC1, Schemas.Message(1, 10, 21));
            var serverSchema = Schemas.Of(0x5E, Schemas.Message(1, 10, 20));
            var link = Link(TransportKind.Message, clientSchema, serverSchema);
            link.Settle(TimeSpan.Zero);

            Check.Equal(
                HandshakeFailure.SchemaConflict,
                Events.First(link.ClientEvents, CycloneEventKind.HandshakeFailed).Failure,
                "the client learns the reason");
            Check.Equal(
                HandshakeFailure.SchemaConflict,
                Events.First(link.ServerEvents, CycloneEventKind.HandshakeFailed).Failure,
                "the server recorded the same reason");
        }

        private static void ServerLifecycle()
        {
            var schema = SharedSchema();
            var listener = new FakeListener();
            using var server = new CycloneServer(listener, schema, Schemas.Config());

            var clientTransport = new FakeTransport(TransportKind.Message);
            var peerTransport = new FakeTransport(TransportKind.Message);
            using var client = new CycloneConnection(
                clientTransport, schema, Schemas.Config(), CycloneRole.Client, TimeSpan.Zero);
            listener.Waiting.Enqueue(peerTransport);

            var collected = new List<CycloneEvent>();
            for (int round = 0; round < 6; round++)
            {
                Pipe.Pump(clientTransport, peerTransport);
                collected.AddRange(server.Tick(TimeSpan.Zero));
                Pipe.Pump(peerTransport, clientTransport);
                client.Tick(TimeSpan.Zero);
            }

            Check.Equal(1, server.PeerCount, "one peer is registered");
            Check.True(Events.Has(collected, CycloneEventKind.Connected), "peer connected");
            Check.True(Events.Has(collected, CycloneEventKind.Ready), "peer ready");
            ulong peerId = Events.First(collected, CycloneEventKind.Ready).PeerId;
            Check.True(peerId != 0, "the peer carries an identifier");
            Check.True(server.IsPeerReady(peerId), "the server agrees the peer is ready");

            client.Send(5, new byte[] { 42 });
            Pipe.Pump(clientTransport, peerTransport);
            var afterMessage = server.Tick(TimeSpan.Zero);
            var message = Events.First(afterMessage, CycloneEventKind.Message);
            Check.Equal(peerId, message.PeerId, "the message names its peer");
            Check.Bytes(new byte[] { 42 }, message.Payload.Span, "payload");

            server.Broadcast(6, new byte[] { 43 });
            Pipe.Pump(peerTransport, clientTransport);
            var atClient = Events.First(client.Tick(TimeSpan.Zero), CycloneEventKind.Message);
            Check.Equal(6u, atClient.MessageId, "the broadcast arrived");

            server.Disconnect(peerId);
            server.Tick(TimeSpan.Zero);
            Check.Equal(0, server.PeerCount, "a closed peer is dropped");
        }

        private static void HeartbeatEndToEnd()
        {
            var link = Link(TransportKind.Message, SharedSchema(), SharedSchema());
            link.Settle(TimeSpan.Zero);

            var now = TimeSpan.Zero;
            for (int round = 0; round < 10; round++)
            {
                now += TimeSpan.FromSeconds(3);
                link.Settle(now);
                Check.False(
                    Events.Has(link.ClientEvents, CycloneEventKind.Disconnected),
                    $"the client is still alive at {now.TotalSeconds}s");
                Check.False(
                    Events.Has(link.ServerEvents, CycloneEventKind.Disconnected),
                    $"the server peer is still alive at {now.TotalSeconds}s");
            }

            Check.Equal(SessionState.Ready, link.Client.State, "client is still ready");
            Check.Equal(SessionState.Ready, link.Server.State, "server peer is still ready");
        }

        private static Schema SharedSchema() =>
            Schemas.Of(0xABCD, Schemas.Message(1, 10, 20), Schemas.Message(2, 30));

        private sealed record Peers(
            CycloneConnection Client,
            FakeTransport ClientTransport,
            CycloneConnection Server,
            FakeTransport ServerTransport)
        {
            public bool Fragment { get; init; }

            public List<CycloneEvent> ClientEvents { get; } = new List<CycloneEvent>();

            public List<CycloneEvent> ServerEvents { get; } = new List<CycloneEvent>();

            public void Settle(TimeSpan now)
            {
                ClientEvents.Clear();
                ServerEvents.Clear();
                for (int round = 0; round < 8; round++)
                {
                    if (Fragment)
                    {
                        Pipe.ExchangeFragmented(ClientTransport, ServerTransport);
                    }
                    else
                    {
                        Pipe.Exchange(ClientTransport, ServerTransport);
                    }
                    var fromClient = Client.Tick(now);
                    bool clientSpoke = fromClient.Count > 0;
                    ClientEvents.AddRange(fromClient);

                    var fromServer = Server.Tick(now);
                    bool serverSpoke = fromServer.Count > 0;
                    ServerEvents.AddRange(fromServer);

                    bool idle = !clientSpoke
                        && !serverSpoke
                        && ClientTransport.Outgoing.Count == 0
                        && ServerTransport.Outgoing.Count == 0
                        && ClientTransport.Incoming.Count == 0
                        && ServerTransport.Incoming.Count == 0;
                    if (idle)
                    {
                        break;
                    }
                }
            }
        }

        private static Peers Link(TransportKind kind, Schema clientSchema, Schema serverSchema)
        {
            var clientTransport = new FakeTransport(kind);
            var serverTransport = new FakeTransport(kind);
            var client = new CycloneConnection(
                clientTransport, clientSchema, Schemas.Config(), CycloneRole.Client, TimeSpan.Zero);
            var server = new CycloneConnection(
                serverTransport, serverSchema, Schemas.Config(), CycloneRole.Server, TimeSpan.Zero);
            return new Peers(client, clientTransport, server, serverTransport);
        }
    }
}
