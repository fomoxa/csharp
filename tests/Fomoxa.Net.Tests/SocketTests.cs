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
            TestRegistry.Add(Group, "UDP carries a 300-message handshake in datagrams of at most 908 bytes", UdpLargeHandshake);
            TestRegistry.Add(Group, "a UDP server keeps two peers apart", UdpTwoPeers);
            TestRegistry.Add(Group, "a full UDP queue loses the oldest datagram, not the newest", UdpQueueDropsOldest);
            TestRegistry.Add(Group, "many unknown sources stop at the peer-table ceiling", UdpPeerTableStopsAtItsCeiling);
            TestRegistry.Add(Group, "a UDP peer keeps its session when its source address changes", UdpPeerMigrates);
            TestRegistry.Add(Group, "a UDP server drops unknown ids and short datagrams, and never moves a peer onto a taken address", UdpServerDropsWhatItShould);
            TestRegistry.Add(Group, "a UDP client keeps the first id it receives", UdpClientKeepsFirstId);
            TestRegistry.Add(Group, "a TCP peer that hangs up is reported as a clean close", TcpPeerHangsUp);
            TestRegistry.Add(Group, "after a peer in the middle leaves, a send by id still reaches the right peer", TcpSendByIdAfterAPeerLeaves);
        }

        private static byte[] Datagram(ulong sessionId, params byte[] frame)
        {
            var datagram = new byte[UdpTransport.HeaderSize + frame.Length];
            BitConverter.GetBytes(sessionId).CopyTo(datagram, 0);
            frame.CopyTo(datagram, UdpTransport.HeaderSize);
            return datagram;
        }

        private static Socket UdpSender() => new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

        private static UdpPeerTransport AcceptFirst(UdpServerTransport listener)
        {
            for (var attempt = 0; attempt < 5_000; attempt++)
            {
                var outcome = listener.Accept();
                if (outcome.Transport != null)
                {
                    return (UdpPeerTransport)outcome.Transport;
                }
                if (outcome.Status == AcceptStatus.Pending)
                {
                    Thread.Sleep(1);
                }
            }
            throw new InvalidOperationException("no peer was accepted");
        }

        private static void Drain(UdpServerTransport listener)
        {
            for (var attempt = 0; attempt < 5_000; attempt++)
            {
                if (listener.Accept().Status == AcceptStatus.Pending)
                {
                    return;
                }
            }
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
                sender.SendTo(Datagram(0, (byte)index), listener.LocalEndPoint);
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
                    sender.SendTo(Datagram(0, 1), listener.LocalEndPoint);
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

        private static void UdpLargeHandshake()
        {
            var messages = new MessageSchema[300];
            for (uint id = 1; id <= messages.Length; id++)
            {
                messages[id - 1] = Schemas.Message(id, id);
            }
            var schema = Schemas.Of(0x5EED, messages);
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            using var client = FomoxaConnection.Connect(
                UdpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero);

            var serverEvents = new List<FomoxaEvent>();
            var clientEvents = new List<FomoxaEvent>();
            Spin(server, client, serverEvents, clientEvents,
                () => client.IsReady && Events.Has(serverEvents, FomoxaEventKind.Ready));

            Check.True(client.IsReady, "the client became ready");
            Check.True(Events.Has(serverEvents, FomoxaEventKind.Ready), "the server accepted the peer");
            Check.True(
                UdpTransport.HeaderSize + FomoxaWire.PacketHandshakeBudget <= 908,
                "a handshake datagram is at most 908 bytes");
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

        private static void UdpPeerMigrates()
        {
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var before = UdpSender();
            using var after = UdpSender();
            before.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            after.Bind(new IPEndPoint(IPAddress.Loopback, 0));

            before.SendTo(Datagram(0, 1, 2), listener.LocalEndPoint);
            var peer = AcceptFirst(listener);
            Check.True(peer.SessionId != 0, "the server assigned a non-zero id");
            var buffer = new byte[64];
            Check.Bytes(new byte[] { 1, 2 }, buffer.AsSpan(0, peer.Receive(buffer).Count), "the first frame arrived without the header");

            peer.Send(new byte[] { 9 });
            var reply = new byte[64];
            EndPoint source = new IPEndPoint(IPAddress.Any, 0);
            Thread.Sleep(50);
            int received = before.ReceiveFrom(reply, ref source);
            Check.Equal(UdpTransport.HeaderSize + 1, received, "the reply carries the header and the frame");
            Check.Equal(peer.SessionId, BitConverter.ToUInt64(reply, 0), "the reply carries the id of the peer");

            after.SendTo(Datagram(peer.SessionId, 3), listener.LocalEndPoint);
            Thread.Sleep(50);
            Drain(listener);
            Check.True(peer.Address.Equals(after.LocalEndPoint), "the peer moved to the new address");
            Check.Bytes(new byte[] { 3 }, buffer.AsSpan(0, peer.Receive(buffer).Count), "the frame from the new address arrived");

            peer.Send(new byte[] { 10 });
            Thread.Sleep(50);
            source = new IPEndPoint(IPAddress.Any, 0);
            received = after.ReceiveFrom(reply, ref source);
            Check.Equal(UdpTransport.HeaderSize + 1, received, "the reply now goes to the new address");
        }

        private static void UdpServerDropsWhatItShould()
        {
            using var listener = new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var first = UdpSender();
            using var second = UdpSender();
            first.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            second.Bind(new IPEndPoint(IPAddress.Loopback, 0));

            first.SendTo(Datagram(0, 1), listener.LocalEndPoint);
            var peer = AcceptFirst(listener);
            var buffer = new byte[64];
            peer.Receive(buffer);

            first.SendTo(new byte[UdpTransport.HeaderSize], listener.LocalEndPoint);
            second.SendTo(Datagram(peer.SessionId + 1, 5), listener.LocalEndPoint);
            Thread.Sleep(50);
            Drain(listener);
            Check.Equal(TransportSignal.WouldBlock, peer.Receive(buffer).Signal, "short datagrams and unknown ids reach nobody");
            Check.True(!peer.Address.Equals(second.LocalEndPoint), "an unknown id does not move a peer");

            second.SendTo(Datagram(0, 6), listener.LocalEndPoint);
            var other = AcceptFirst(listener);
            first.SendTo(Datagram(other.SessionId, 7), listener.LocalEndPoint);
            Thread.Sleep(50);
            Drain(listener);
            Check.True(other.Address.Equals(second.LocalEndPoint), "a peer is not moved onto an address another peer holds");
        }

        private static void UdpClientKeepsFirstId()
        {
            using var server = UdpSender();
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            using var client = UdpTransport.Connect((IPEndPoint)server.LocalEndPoint!);

            client.Send(new byte[] { 1 });
            var request = new byte[64];
            EndPoint from = new IPEndPoint(IPAddress.Any, 0);
            Thread.Sleep(50);
            server.ReceiveFrom(request, ref from);
            Check.Equal(0UL, BitConverter.ToUInt64(request, 0), "the client sends id 0 before it knows its id");

            server.SendTo(Datagram(0, 2), from);
            server.SendTo(Datagram(11, 3), from);
            server.SendTo(Datagram(22, 4), from);
            Thread.Sleep(50);

            var buffer = new byte[64];
            Check.Equal(TransportSignal.Ok, client.Receive(buffer).Signal, "the first frame with a non-zero id arrived");
            Check.Equal(TransportSignal.WouldBlock, client.Receive(buffer).Signal, "a frame with another id was dropped");

            client.Send(new byte[] { 8 });
            Thread.Sleep(50);
            server.ReceiveFrom(request, ref from);
            Check.Equal(11UL, BitConverter.ToUInt64(request, 0), "the client now sends the id it received");
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

        private static void TcpSendByIdAfterAPeerLeaves()
        {
            var schema = Shared();
            using var listener = new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, 0));
            using var server = new FomoxaServer(listener, schema, Schemas.Config());
            var clients = new List<FomoxaConnection>();
            for (int index = 0; index < 3; index++)
            {
                clients.Add(FomoxaConnection.Connect(
                    TcpTransport.Connect(listener.LocalEndPoint), schema, Schemas.Config(), TimeSpan.Zero));
            }

            try
            {
                var serverEvents = new List<FomoxaEvent>();
                for (int round = 0; round < 400 && Events.Count(serverEvents, FomoxaEventKind.Ready) < 3; round++)
                {
                    serverEvents.AddRange(server.Tick(TimeSpan.Zero));
                    foreach (var client in clients)
                    {
                        client.Tick(TimeSpan.Zero);
                    }
                    Thread.Sleep(2);
                }
                Check.Equal(3, Events.Count(serverEvents, FomoxaEventKind.Ready), "three peers became ready");

                var ids = new List<ulong>(server.PeerIds());
                foreach (ulong id in ids)
                {
                    Check.Equal(SendStatus.Sent, server.Send(id, 3, new byte[] { (byte)id }), "server sends to each peer");
                }
                var idOfClient = new ulong[clients.Count];
                for (int index = 0; index < clients.Count; index++)
                {
                    idOfClient[index] = ReceiveOne(server, clients[index]).Span[0];
                }

                ulong leaving = ids[1];
                int leavingClient = Array.IndexOf(idOfClient, leaving);
                clients[leavingClient].Close();
                clients[leavingClient].Dispose();
                clients.RemoveAt(leavingClient);
                var remainingIds = new List<ulong>(idOfClient);
                remainingIds.RemoveAt(leavingClient);

                for (int round = 0; round < 400 && server.PeerCount > 2; round++)
                {
                    server.Tick(TimeSpan.Zero);
                    Thread.Sleep(2);
                }
                Check.Equal(2, server.PeerCount, "the leaving peer was dropped");
                Check.Equal(SendStatus.Closed, server.Send(leaving, 3, new byte[] { 0 }), "a send to the dropped peer");

                for (int index = clients.Count - 1; index >= 0; index--)
                {
                    Check.Equal(
                        SendStatus.Sent,
                        server.Send(remainingIds[index], 3, new byte[] { (byte)(0x80 | index) }),
                        "server sends to a remaining peer");
                }
                for (int index = 0; index < clients.Count; index++)
                {
                    Check.Bytes(
                        new byte[] { (byte)(0x80 | index) },
                        ReceiveOne(server, clients[index]).Span,
                        "each remaining client gets the message sent to its id");
                }
            }
            finally
            {
                foreach (var client in clients)
                {
                    client.Dispose();
                }
            }
        }

        private static ReadOnlyMemory<byte> ReceiveOne(FomoxaServer server, FomoxaConnection client)
        {
            var clientEvents = new List<FomoxaEvent>();
            for (int round = 0; round < 400 && !Events.Has(clientEvents, FomoxaEventKind.Message); round++)
            {
                server.Tick(TimeSpan.Zero);
                clientEvents.AddRange(client.Tick(TimeSpan.Zero));
                Thread.Sleep(2);
            }
            Check.Equal(1, Events.Count(clientEvents, FomoxaEventKind.Message), "the client got one message");
            return Events.First(clientEvents, FomoxaEventKind.Message).Payload;
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
