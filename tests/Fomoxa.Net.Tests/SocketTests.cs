using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Tests
{
    public static class SocketTests
    {
        private const string Group = "real sockets on loopback";

        public static void Register()
        {
            TestRegistry.Add(Group, "TCP carries a handshake and a message each way", TcpRoundTrip);
            TestRegistry.Add(Group, "UDP carries a handshake and a message each way", UdpRoundTrip);
            TestRegistry.Add(Group, "a UDP server keeps two peers apart", UdpTwoPeers);
            TestRegistry.Add(Group, "a full UDP queue loses the oldest datagram, not the newest", UdpQueueDropsOldest);
            TestRegistry.Add(Group, "many unknown sources stop at the peer-table ceiling", UdpPeerTableStopsAtItsCeiling);
            TestRegistry.Add(Group, "a TCP peer that hangs up is reported as a clean close", TcpPeerHangsUp);
        }

        private static Schema Shared() => Schemas.Of(0xABCD, Schemas.Message(1, 10, 20));

        private static void TcpRoundTrip()
        {
            var schema = Shared();
            using var listener = new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            using var client = FomoxaConnection.Connect(
                TcpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<FomoxaEvent>();
            var clientEvents = new List<FomoxaEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, FomoxaEventKind.Ready));

            Check.True(client.IsReady, "the client became ready");
            Check.True(Events.Has(serverEvents, FomoxaEventKind.Ready), "the server's peer became ready");
            ulong peerId = Events.First(serverEvents, FomoxaEventKind.Ready).PeerId;

            Check.Equal(SendStatus.Sent, client.Send(1, new byte[] { 1, 2, 3 }), "client sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(serverEvents, FomoxaEventKind.Message));
            var atServer = Events.First(serverEvents, FomoxaEventKind.Message);
            Check.Bytes(new byte[] { 1, 2, 3 }, atServer.Payload.Span, "payload at the server");

            Check.Equal(SendStatus.Sent, server.Send(peerId, 2, new byte[] { 4, 5 }), "server sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(clientEvents, FomoxaEventKind.Message));
            var atClient = Events.First(clientEvents, FomoxaEventKind.Message);
            Check.Equal(2u, atClient.MessageId, "message id at the client");
            Check.Bytes(new byte[] { 4, 5 }, atClient.Payload.Span, "payload at the client");
        }

        // 01 §6: the queue is bounded and the oldest packet is the one that
        // goes, so the freshest data survives a burst.
        private static void UdpQueueDropsOldest()
        {
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

            const int overflow = 8;
            for (var index = 0; index < UdpPeerTransport.QueueCeiling + overflow; index++)
            {
                sender.SendTo(new[] { (byte)index }, listener.LocalEndPoint);
            }
            Thread.Sleep(200);

            ITransport? peer = null;
            for (var attempt = 0; attempt < 5_000; attempt++)
            {
                var outcome = listener.Accept();
                if (outcome.Transport != null)
                {
                    peer = outcome.Transport;
                }
                if (outcome.Status == AcceptStatus.Pending && peer != null)
                {
                    break;
                }
            }
            Check.True(peer != null, "the sender became a peer");

            var buffer = new byte[64];
            var first = peer!.Receive(buffer);
            Check.Equal(TransportSignal.Ok, first.Signal, "a datagram is waiting");
            Check.Equal(
                overflow,
                buffer[0],
                "the first datagram still held is the one after the ones that were dropped");
        }

        // 01 §10: past the ceiling an unknown address is treated exactly like
        // an unexpected packet - dropped silently, nothing else disturbed.
        private static void UdpPeerTableStopsAtItsCeiling()
        {
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));

            var senders = new List<Socket>();
            try
            {
                for (var index = 0; index < 12; index++)
                {
                    var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                    senders.Add(sender);
                    sender.SendTo(new byte[] { 1 }, listener.LocalEndPoint);
                }
                Thread.Sleep(200);

                var accepted = 0;
                for (var attempt = 0; attempt < 5_000; attempt++)
                {
                    var outcome = listener.Accept();
                    if (outcome.Transport != null)
                    {
                        accepted++;
                    }
                    if (outcome.Status == AcceptStatus.Pending)
                    {
                        break;
                    }
                }

                Check.True(accepted > 0, "genuine new peers are still accepted");
                Check.True(
                    accepted <= UdpServerTransport.PeerCeiling,
                    "the peer table never passes its ceiling");
            }
            finally
            {
                foreach (var sender in senders)
                {
                    sender.Dispose();
                }
            }
        }

        private static void UdpRoundTrip()
        {
            var schema = Shared();
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            using var client = FomoxaConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<FomoxaEvent>();
            var clientEvents = new List<FomoxaEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, FomoxaEventKind.Ready));

            Check.True(client.IsReady, "the client became ready");
            ulong peerId = Events.First(serverEvents, FomoxaEventKind.Ready).PeerId;

            Check.Equal(SendStatus.Sent, client.Send(1, new byte[] { 7, 7 }), "client sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(serverEvents, FomoxaEventKind.Message));
            Check.Bytes(
                new byte[] { 7, 7 },
                Events.First(serverEvents, FomoxaEventKind.Message).Payload.Span,
                "payload at the server");

            Check.Equal(SendStatus.Sent, server.Send(peerId, 3, new byte[] { 8 }), "server sends");
            Spin(server, client, serverEvents, clientEvents,
                () => Events.Has(clientEvents, FomoxaEventKind.Message));
            Check.Bytes(
                new byte[] { 8 },
                Events.First(clientEvents, FomoxaEventKind.Message).Payload.Span,
                "payload at the client");
        }

        private static void UdpTwoPeers()
        {
            var schema = Shared();
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            using var first = FomoxaConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);
            using var second = FomoxaConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<FomoxaEvent>();
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
            var collected = new List<FomoxaEvent>();
            for (int round = 0; round < 400 && !Events.Has(collected, FomoxaEventKind.Message); round++)
            {
                collected.AddRange(server.Tick(TimeSpan.Zero));
                Thread.Sleep(2);
            }

            var message = Events.First(collected, FomoxaEventKind.Message);
            Check.Bytes(new byte[] { 0x11 }, message.Payload.Span, "payload");
            Check.Equal(1, Events.Count(collected, FomoxaEventKind.Message), "only one peer sent anything");
        }

        private static void TcpPeerHangsUp()
        {
            var schema = Shared();
            using var listener = new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            var client = FomoxaConnection.Connect(
                TcpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<FomoxaEvent>();
            var clientEvents = new List<FomoxaEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, FomoxaEventKind.Ready));

            client.Close();
            client.Dispose();

            for (int round = 0; round < 400 && !Events.Has(serverEvents, FomoxaEventKind.Disconnected); round++)
            {
                serverEvents.AddRange(server.Tick(TimeSpan.Zero));
                Thread.Sleep(2);
            }

            Check.Equal(
                DisconnectReason.PeerClosed,
                Events.First(serverEvents, FomoxaEventKind.Disconnected).Reason,
                "the server saw a clean close");
            Check.Equal(0, server.PeerCount, "the peer was dropped");
        }

        private static void Spin(
            FomoxaServer server,
            FomoxaConnection client,
            List<FomoxaEvent> serverEvents,
            List<FomoxaEvent> clientEvents,
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
