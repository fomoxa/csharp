using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Cyclone.Net;
using Cyclone.Net.Transports;

namespace Cyclone.Net.Tests
{
    public static class SocketTests
    {
        private const string Group = "real sockets on loopback";

        public static void Register()
        {
            TestRegistry.Add(Group, "TCP carries a handshake and a message each way", TcpRoundTrip);
            TestRegistry.Add(Group, "UDP carries a handshake and a message each way", UdpRoundTrip);
            TestRegistry.Add(Group, "a UDP server keeps two peers apart", UdpTwoPeers);
            TestRegistry.Add(Group, "a TCP peer that hangs up is reported as a clean close", TcpPeerHangsUp);
        }

        private static Schema Shared() => Schemas.Of(0xABCD, Schemas.Message(1, 10, 20));

        private static void TcpRoundTrip()
        {
            var schema = Shared();
            using var listener = new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new CycloneServer(listener, schema, Schemas.Config());
            using var client = CycloneConnection.Connect(
                TcpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<CycloneEvent>();
            var clientEvents = new List<CycloneEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, CycloneEventKind.Ready));

            Check.True(client.IsReady, "the client became ready");
            Check.True(Events.Has(serverEvents, CycloneEventKind.Ready), "the server's peer became ready");
            ulong peerId = Events.First(serverEvents, CycloneEventKind.Ready).PeerId;

            Check.Equal(SendStatus.Sent, client.Send(1, new byte[] { 1, 2, 3 }), "client sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(serverEvents, CycloneEventKind.Message));
            var atServer = Events.First(serverEvents, CycloneEventKind.Message);
            Check.Bytes(new byte[] { 1, 2, 3 }, atServer.Payload.Span, "payload at the server");

            Check.Equal(SendStatus.Sent, server.Send(peerId, 2, new byte[] { 4, 5 }), "server sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(clientEvents, CycloneEventKind.Message));
            var atClient = Events.First(clientEvents, CycloneEventKind.Message);
            Check.Equal(2u, atClient.MessageId, "message id at the client");
            Check.Bytes(new byte[] { 4, 5 }, atClient.Payload.Span, "payload at the client");
        }

        private static void UdpRoundTrip()
        {
            var schema = Shared();
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new CycloneServer(listener, schema, Schemas.Config());
            using var client = CycloneConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<CycloneEvent>();
            var clientEvents = new List<CycloneEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, CycloneEventKind.Ready));

            Check.True(client.IsReady, "the client became ready");
            ulong peerId = Events.First(serverEvents, CycloneEventKind.Ready).PeerId;

            Check.Equal(SendStatus.Sent, client.Send(1, new byte[] { 7, 7 }), "client sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(serverEvents, CycloneEventKind.Message));
            Check.Bytes(
                new byte[] { 7, 7 },
                Events.First(serverEvents, CycloneEventKind.Message).Payload.Span,
                "payload at the server");

            Check.Equal(SendStatus.Sent, server.Send(peerId, 3, new byte[] { 8 }), "server sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(clientEvents, CycloneEventKind.Message));
            Check.Bytes(
                new byte[] { 8 },
                Events.First(clientEvents, CycloneEventKind.Message).Payload.Span,
                "payload at the client");
        }

        private static void UdpTwoPeers()
        {
            var schema = Shared();
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new CycloneServer(listener, schema, Schemas.Config());
            using var first = CycloneConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);
            using var second = CycloneConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<CycloneEvent>();
            for (int round = 0; round < 400 && (!first.IsReady || !second.IsReady); round++)
            {
                serverEvents.AddRange(server.Tick(TimeSpan.Zero));
                first.Tick(TimeSpan.Zero);
                second.Tick(TimeSpan.Zero);
                Thread.Sleep(2);
            }

            Check.True(first.IsReady, "the first client became ready");
            Check.True(second.IsReady, "the second client became ready");
            Check.Equal(2, server.PeerCount, "the server holds two separate peers");

            first.Send(1, new byte[] { 0x11 });
            var collected = new List<CycloneEvent>();
            for (int round = 0; round < 400 && !Events.Has(collected, CycloneEventKind.Message); round++)
            {
                collected.AddRange(server.Tick(TimeSpan.Zero));
                Thread.Sleep(2);
            }

            var message = Events.First(collected, CycloneEventKind.Message);
            Check.Bytes(new byte[] { 0x11 }, message.Payload.Span, "payload");
            Check.Equal(1, Events.Count(collected, CycloneEventKind.Message), "only one peer sent anything");
        }

        private static void TcpPeerHangsUp()
        {
            var schema = Shared();
            using var listener = new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new CycloneServer(listener, schema, Schemas.Config());
            var client = CycloneConnection.Connect(
                TcpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<CycloneEvent>();
            var clientEvents = new List<CycloneEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, CycloneEventKind.Ready));

            client.Close();
            client.Dispose();

            for (int round = 0; round < 400 && !Events.Has(serverEvents, CycloneEventKind.Disconnected); round++)
            {
                serverEvents.AddRange(server.Tick(TimeSpan.Zero));
                Thread.Sleep(2);
            }

            Check.Equal(
                DisconnectReason.PeerClosed,
                Events.First(serverEvents, CycloneEventKind.Disconnected).Reason,
                "the server saw a clean close");
            Check.Equal(0, server.PeerCount, "the peer was dropped");
        }

        private static void Spin(
            CycloneServer server,
            CycloneConnection client,
            List<CycloneEvent> serverEvents,
            List<CycloneEvent> clientEvents,
            Func<bool> until)
        {
            for (int round = 0; round < 400; round++)
            {
                serverEvents.AddRange(server.Tick(TimeSpan.Zero));
                clientEvents.AddRange(client.Tick(TimeSpan.Zero));
                if (until())
                {
                    return;
                }
                Thread.Sleep(2);
            }
            throw new AssertionException("the two peers never reached the expected state");
        }
    }
}
