using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Reliability.Tests
{
    public sealed class AssertionException : Exception
    {
        public AssertionException(string message) : base(message)
        {
        }
    }

    public static class Check
    {
        public static void True(bool condition, string what)
        {
            if (!condition)
            {
                throw new AssertionException(what);
            }
        }

        public static void Equal<T>(T expected, T actual, string what)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
            {
                throw new AssertionException($"{what}: expected {expected}, got {actual}");
            }
        }
    }

    /// In-memory ITransport (Kind = Message) with an explicit Outgoing/Incoming
    /// queue, so a test can pump frames between two peers by hand and, unlike a
    /// real socket, selectively drop a specific outgoing frame to simulate the
    /// packet loss a real UDP link would eventually deliver anyway.
    public sealed class FakeTransport : ITransport
    {
        public TransportKind Kind => TransportKind.Message;

        public Queue<byte[]> Outgoing { get; } = new Queue<byte[]>();

        public Queue<byte[]> Incoming { get; } = new Queue<byte[]>();

        public Func<byte[], bool>? DropOutgoing { get; set; }

        public int BlockNextSends { get; set; }

        /// Simulates this side's own socket reporting the peer gone, the way a
        /// real transport would - Fomoxa's wire format has no close frame of its
        /// own (01-overview.md §9), so a peer's death is only ever observed
        /// through Send/Receive outcomes on the transport itself, never through
        /// anything arriving from the other side's FakeTransport.
        public bool ReportClosed { get; set; }

        public SendOutcome Send(ReadOnlySpan<byte> bytes)
        {
            if (ReportClosed)
            {
                return SendOutcome.Closed;
            }

            if (BlockNextSends > 0)
            {
                BlockNextSends--;
                return SendOutcome.WouldBlock;
            }

            var copy = bytes.ToArray();
            if (DropOutgoing != null && DropOutgoing(copy))
            {
                return SendOutcome.Ok;
            }

            Outgoing.Enqueue(copy);
            return SendOutcome.Ok;
        }

        public ReceiveOutcome Receive(Span<byte> buffer)
        {
            if (Incoming.Count == 0)
            {
                return ReportClosed ? ReceiveOutcome.Closed : ReceiveOutcome.WouldBlock;
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

        public void CloseGracefully()
        {
        }

        public void Dispose()
        {
        }
    }

    public sealed class FakeListener : IListenerTransport
    {
        public Queue<ITransport> Waiting { get; } = new Queue<ITransport>();

        public AcceptOutcome Accept() =>
            Waiting.Count > 0 ? AcceptOutcome.Accepted(Waiting.Dequeue()) : AcceptOutcome.Pending;

        public void Dispose()
        {
        }
    }

    public static class Pipe
    {
        public static void Exchange(FakeTransport left, FakeTransport right)
        {
            while (left.Outgoing.Count > 0)
            {
                right.Incoming.Enqueue(left.Outgoing.Dequeue());
            }

            while (right.Outgoing.Count > 0)
            {
                left.Incoming.Enqueue(right.Outgoing.Dequeue());
            }
        }
    }

    /// Reads the messageId out of a raw DATA frame, the same layout the wire
    /// format uses ([type][F][O][messageId LE][length LE][payload]) - used by
    /// tests to decide whether a given outgoing frame is the reliability
    /// envelope/ack before deciding whether to drop it.
    public static class FrameSniff
    {
        public static bool TryMessageId(byte[] frame, out uint messageId)
        {
            const int dataFrameType = 0;
            if (frame.Length < 11 || frame[0] != dataFrameType)
            {
                messageId = 0;
                return false;
            }

            messageId = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(3, 4));
            return true;
        }
    }
}
