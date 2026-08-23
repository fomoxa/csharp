using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace Fomoxa.Net.Transports
{
    public sealed class UdpTransport : ITransport
    {
        internal const int MaxDatagram = 65535;
        internal const int MaxPayload = 65507;

        private readonly Socket socket;
        private readonly IPEndPoint peer;
        private readonly byte[] inbox = new byte[MaxDatagram];
        private readonly byte[] sendScratch = new byte[MaxDatagram];
        private int inboxLength = -1;
        private bool released;

        public UdpTransport(Socket socket, IPEndPoint peer)
        {
            this.socket = socket ?? throw new ArgumentNullException(nameof(socket));
            this.peer = peer ?? throw new ArgumentNullException(nameof(peer));
            this.socket.Blocking = false;
            UdpSocket.SilenceConnectionReset(this.socket);
        }

        public static UdpTransport Connect(IPEndPoint remote)
        {
            if (remote == null)
            {
                throw new ArgumentNullException(nameof(remote));
            }
            var socket = UdpSocket.Create(remote.AddressFamily);
            socket.Bind(new IPEndPoint(UdpSocket.AnyAddress(remote.AddressFamily), 0));
            return new UdpTransport(socket, remote);
        }

        public TransportKind Kind => TransportKind.Message;

        public Socket Socket => socket;

        public IPEndPoint Peer => peer;

        public SendOutcome Send(ReadOnlySpan<byte> bytes)
        {
            if (released)
            {
                return SendOutcome.Closed;
            }
            if (bytes.Length > MaxPayload)
            {
                return SendOutcome.TooLarge;
            }
            bytes.CopyTo(sendScratch);
            return UdpSocket.SendTo(socket, sendScratch, bytes.Length, peer);
        }

        public ReceiveOutcome Receive(Span<byte> buffer)
        {
            if (released)
            {
                return ReceiveOutcome.Closed;
            }

            if (inboxLength < 0)
            {
                while (true)
                {
                    var status = UdpSocket.ReceiveFrom(socket, inbox, out int count, out EndPoint from);
                    if (status == UdpReceiveStatus.WouldBlock)
                    {
                        return ReceiveOutcome.WouldBlock;
                    }
                    if (status == UdpReceiveStatus.Closed)
                    {
                        return ReceiveOutcome.Closed;
                    }
                    if (status == UdpReceiveStatus.Error)
                    {
                        return ReceiveOutcome.Error;
                    }
                    if (status == UdpReceiveStatus.Discard)
                    {
                        continue;
                    }
                    if (!peer.Equals(from))
                    {
                        continue;
                    }
                    inboxLength = count;
                    break;
                }
            }

            if (inboxLength > buffer.Length)
            {
                return ReceiveOutcome.NeedCapacity(inboxLength);
            }
            new ReadOnlySpan<byte>(inbox, 0, inboxLength).CopyTo(buffer);
            int delivered = inboxLength;
            inboxLength = -1;
            return ReceiveOutcome.Ok(delivered);
        }

        public void CloseGracefully()
        {
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            socket.Dispose();
        }
    }

    public sealed class UdpServerTransport : IListenerTransport
    {
        internal const int PeerCeiling = 1024;

        private readonly Socket socket;
        private readonly Dictionary<IPEndPoint, UdpPeerTransport> peers =
            new Dictionary<IPEndPoint, UdpPeerTransport>();
        private readonly byte[] receiveScratch = new byte[UdpTransport.MaxDatagram];
        private readonly byte[] sendScratch = new byte[UdpTransport.MaxDatagram];
        private bool released;

        public UdpServerTransport(IPEndPoint local)
        {
            if (local == null)
            {
                throw new ArgumentNullException(nameof(local));
            }
            socket = UdpSocket.Create(local.AddressFamily);
            socket.Bind(local);
            socket.Blocking = false;
            UdpSocket.SilenceConnectionReset(socket);
        }

        public IPEndPoint LocalEndPoint => (IPEndPoint)socket.LocalEndPoint;

        public AcceptOutcome Accept()
        {
            if (released)
            {
                return AcceptOutcome.Error;
            }

            var status = UdpSocket.ReceiveFrom(socket, receiveScratch, out int count, out EndPoint from);
            switch (status)
            {
                case UdpReceiveStatus.WouldBlock:
                    return AcceptOutcome.Pending;
                case UdpReceiveStatus.Discard:
                    return AcceptOutcome.Progress;
                case UdpReceiveStatus.Closed:
                case UdpReceiveStatus.Error:
                    return AcceptOutcome.Error;
            }

            var address = (IPEndPoint)from;
            if (peers.TryGetValue(address, out var known))
            {
                known.Enqueue(receiveScratch, count);
                return AcceptOutcome.Progress;
            }

            // A UDP port hears from anyone, so an unbounded peer table is a
            // stream of unknown source addresses away from exhausting memory.
            // At the ceiling a new address is treated exactly like an
            // unexpected packet: dropped silently, running sessions untouched
            // (01 §10).
            if (peers.Count >= PeerCeiling)
            {
                return AcceptOutcome.Progress;
            }

            var created = new UdpPeerTransport(this, address);
            created.Enqueue(receiveScratch, count);
            peers.Add(address, created);
            return AcceptOutcome.Accepted(created);
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            peers.Clear();
            socket.Dispose();
        }

        internal SendOutcome SendTo(IPEndPoint address, ReadOnlySpan<byte> bytes)
        {
            if (released)
            {
                return SendOutcome.Closed;
            }
            if (bytes.Length > UdpTransport.MaxPayload)
            {
                return SendOutcome.TooLarge;
            }
            bytes.CopyTo(sendScratch);
            return UdpSocket.SendTo(socket, sendScratch, bytes.Length, address);
        }

        internal void Forget(IPEndPoint address) => peers.Remove(address);
    }

    public sealed class UdpPeerTransport : ITransport
    {
        internal const int QueueCeiling = 64;

        private readonly UdpServerTransport owner;
        private readonly IPEndPoint address;
        private readonly Queue<byte[]> inbox = new Queue<byte[]>();
        private bool released;

        internal UdpPeerTransport(UdpServerTransport owner, IPEndPoint address)
        {
            this.owner = owner;
            this.address = address;
        }

        public TransportKind Kind => TransportKind.Message;

        public IPEndPoint Address => address;

        public SendOutcome Send(ReadOnlySpan<byte> bytes) =>
            released ? SendOutcome.Closed : owner.SendTo(address, bytes);

        public ReceiveOutcome Receive(Span<byte> buffer)
        {
            if (inbox.Count == 0)
            {
                return released ? ReceiveOutcome.Closed : ReceiveOutcome.WouldBlock;
            }

            var packet = inbox.Peek();
            if (packet.Length > buffer.Length)
            {
                return ReceiveOutcome.NeedCapacity(packet.Length);
            }
            packet.CopyTo(buffer);
            inbox.Dequeue();
            return ReceiveOutcome.Ok(packet.Length);
        }

        public void CloseGracefully()
        {
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            inbox.Clear();
            owner.Forget(address);
        }

        internal void Enqueue(byte[] source, int count)
        {
            // The oldest goes, not the newest: a real-time peer is better
            // served by fresh data, and transport may not read the payload to
            // decide otherwise (01 §6, §12).
            if (inbox.Count >= QueueCeiling)
            {
                inbox.Dequeue();
            }
            var packet = new byte[count];
            Buffer.BlockCopy(source, 0, packet, 0, count);
            inbox.Enqueue(packet);
        }
    }

    internal enum UdpReceiveStatus
    {
        Received,
        WouldBlock,
        Discard,
        Closed,
        Error,
    }

    internal static class UdpSocket
    {
        private const int SioUdpConnectionReset = -1744830452;

        public static Socket Create(AddressFamily family) =>
            new Socket(family, SocketType.Dgram, ProtocolType.Udp);

        public static IPAddress AnyAddress(AddressFamily family) =>
            family == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any;

        public static void SilenceConnectionReset(Socket socket)
        {
            try
            {
                socket.IOControl(SioUdpConnectionReset, new byte[] { 0, 0, 0, 0 }, null);
            }
            catch (SocketException)
            {
            }
            catch (PlatformNotSupportedException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public static SendOutcome SendTo(Socket socket, byte[] bytes, int length, IPEndPoint address)
        {
            try
            {
                socket.SendTo(bytes, 0, length, SocketFlags.None, address);
                return SendOutcome.Ok;
            }
            catch (SocketException error)
            {
                switch (error.SocketErrorCode)
                {
                    case SocketError.WouldBlock:
                    case SocketError.NoBufferSpaceAvailable:
                        return SendOutcome.WouldBlock;
                    case SocketError.MessageSize:
                        return SendOutcome.TooLarge;
                    case SocketError.ConnectionReset:
                        return SendOutcome.Ok;
                    default:
                        return SendOutcome.Error;
                }
            }
            catch (ObjectDisposedException)
            {
                return SendOutcome.Closed;
            }
        }

        public static UdpReceiveStatus ReceiveFrom(
            Socket socket, byte[] buffer, out int count, out EndPoint from)
        {
            from = new IPEndPoint(AnyAddress(socket.AddressFamily), 0);
            count = 0;
            try
            {
                count = socket.ReceiveFrom(buffer, 0, buffer.Length, SocketFlags.None, ref from);
                return UdpReceiveStatus.Received;
            }
            catch (SocketException error)
            {
                switch (error.SocketErrorCode)
                {
                    case SocketError.WouldBlock:
                        return UdpReceiveStatus.WouldBlock;
                    case SocketError.ConnectionReset:
                    case SocketError.MessageSize:
                        return UdpReceiveStatus.Discard;
                    default:
                        return UdpReceiveStatus.Error;
                }
            }
            catch (ObjectDisposedException)
            {
                return UdpReceiveStatus.Closed;
            }
        }
    }
}
