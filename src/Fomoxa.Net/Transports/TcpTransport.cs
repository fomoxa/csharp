using System;
using System.Net;
using System.Net.Sockets;

namespace Fomoxa.Net.Transports
{
    public sealed class TcpTransport : ITransport
    {
        private readonly Socket socket;
        private byte[] tail = Array.Empty<byte>();
        private int tailUsed;
        private bool errored;
        private bool peerClosed;
        private bool released;

        public TcpTransport(Socket socket)
        {
            this.socket = socket ?? throw new ArgumentNullException(nameof(socket));
            this.socket.Blocking = false;
            this.socket.NoDelay = true;
        }

        public static TcpTransport Connect(IPEndPoint remote)
        {
            if (remote == null)
            {
                throw new ArgumentNullException(nameof(remote));
            }
            var socket = new Socket(remote.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(remote);
            return new TcpTransport(socket);
        }

        public static TcpTransport Connect(string host, int port) =>
            Connect(new IPEndPoint(Resolve(host), port));

        public TransportKind Kind => TransportKind.Stream;

        public Socket Socket => socket;

        public SendOutcome Send(ReadOnlySpan<byte> bytes)
        {
            if (released)
            {
                return SendOutcome.Closed;
            }
            if (errored)
            {
                return SendOutcome.Error;
            }

            if (!FlushTail())
            {
                return errored ? SendOutcome.Error : SendOutcome.WouldBlock;
            }
            if (bytes.Length == 0)
            {
                return SendOutcome.Ok;
            }

            int sent = socket.Send(bytes, SocketFlags.None, out SocketError error);
            if (error != SocketError.Success && error != SocketError.WouldBlock)
            {
                errored = true;
                return SendOutcome.Error;
            }
            if (sent <= 0)
            {
                return SendOutcome.WouldBlock;
            }
            if (sent < bytes.Length)
            {
                KeepTail(bytes.Slice(sent));
            }
            return SendOutcome.Ok;
        }

        public ReceiveOutcome Receive(Span<byte> buffer)
        {
            if (released)
            {
                return ReceiveOutcome.Closed;
            }
            if (errored)
            {
                return ReceiveOutcome.Error;
            }
            if (peerClosed)
            {
                return ReceiveOutcome.Closed;
            }

            FlushTail();
            if (errored)
            {
                return ReceiveOutcome.Error;
            }
            if (buffer.Length == 0)
            {
                return ReceiveOutcome.WouldBlock;
            }

            int read = socket.Receive(buffer, SocketFlags.None, out SocketError error);
            if (error == SocketError.WouldBlock)
            {
                return ReceiveOutcome.WouldBlock;
            }
            if (error != SocketError.Success)
            {
                errored = true;
                return ReceiveOutcome.Error;
            }
            if (read == 0)
            {
                peerClosed = true;
                return ReceiveOutcome.Closed;
            }
            return ReceiveOutcome.Ok(read);
        }

        public void CloseGracefully()
        {
            if (released || errored)
            {
                return;
            }
            try
            {
                socket.Shutdown(SocketShutdown.Send);
            }
            catch (SocketException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            tail = Array.Empty<byte>();
            tailUsed = 0;
            socket.Dispose();
        }

        private bool FlushTail()
        {
            while (tailUsed > 0)
            {
                int sent = socket.Send(
                    new ReadOnlySpan<byte>(tail, 0, tailUsed), SocketFlags.None, out SocketError error);
                if (error != SocketError.Success && error != SocketError.WouldBlock)
                {
                    errored = true;
                    return false;
                }
                if (sent <= 0)
                {
                    return false;
                }
                if (sent >= tailUsed)
                {
                    tailUsed = 0;
                    break;
                }
                Buffer.BlockCopy(tail, sent, tail, 0, tailUsed - sent);
                tailUsed -= sent;
            }
            return true;
        }

        private void KeepTail(ReadOnlySpan<byte> remainder)
        {
            if (tail.Length < remainder.Length)
            {
                tail = new byte[remainder.Length];
            }
            remainder.CopyTo(tail);
            tailUsed = remainder.Length;
        }

        private static IPAddress Resolve(string host)
        {
            if (IPAddress.TryParse(host, out var parsed))
            {
                return parsed;
            }
            var addresses = Dns.GetHostAddresses(host);
            if (addresses.Length == 0)
            {
                throw new SocketException((int)SocketError.HostNotFound);
            }
            return addresses[0];
        }
    }

    public sealed class TcpListenerTransport : IListenerTransport
    {
        private readonly Socket socket;
        private bool released;

        public TcpListenerTransport(IPEndPoint local, int backlog = 128)
        {
            if (local == null)
            {
                throw new ArgumentNullException(nameof(local));
            }
            socket = new Socket(local.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(local);
            socket.Listen(backlog);
            socket.Blocking = false;
        }

        public IPEndPoint LocalEndPoint => (IPEndPoint)socket.LocalEndPoint;

        public AcceptOutcome Accept()
        {
            if (released)
            {
                return AcceptOutcome.Error;
            }
            try
            {
                var peer = socket.Accept();
                return AcceptOutcome.Accepted(new TcpTransport(peer));
            }
            catch (SocketException error) when (error.SocketErrorCode == SocketError.WouldBlock)
            {
                return AcceptOutcome.Pending;
            }
            catch (SocketException)
            {
                return AcceptOutcome.Error;
            }
            catch (ObjectDisposedException)
            {
                return AcceptOutcome.Error;
            }
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
}
