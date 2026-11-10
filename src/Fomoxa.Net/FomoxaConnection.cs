using System;
using System.Collections.Generic;
using Fomoxa.Net.Framing;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net
{
    public enum SendStatus
    {
        Sent,
        NotReady,
        Congested,
        TooLarge,
        Closed,
    }

    public sealed class FomoxaConnection : IDisposable
    {
        private readonly ITransport transport;
        private readonly IFrameSource frameSource;
        private readonly Session session;
        private readonly SessionConfig config;
        private readonly ulong peerId;
        private readonly List<FomoxaEvent> events = new List<FomoxaEvent>();

        private const int DefaultEventPayloadCapacity = 4096;
        private const int MaxOutboxBytes = 64 * 1024;

        private byte[] eventPayloads = new byte[DefaultEventPayloadCapacity];
        private int eventPayloadsUsed;
        private byte[] outbox = new byte[0];
        private int outboxUsed;
        private byte[] scratch = new byte[0];
        private byte[] partBuffer = Array.Empty<byte>();
        private readonly bool packetTransport;
        private readonly HandshakeAssembler assembler = new HandshakeAssembler();
        private readonly Queue<PendingPacket> pendingPackets = new Queue<PendingPacket>();
        private int pendingCountedBytes;

        private bool transportDead;
        private bool gracefulDeath;
        private bool overloaded;
        private bool connectedEmitted;
        private bool released;

        public FomoxaConnection(
            ITransport transport, Schema schema, SessionConfig config, FomoxaRole role, TimeSpan now)
            : this(transport, schema, config, role, now, 0)
        {
        }

        internal FomoxaConnection(
            ITransport transport,
            Schema schema,
            SessionConfig config,
            FomoxaRole role,
            TimeSpan now,
            ulong peerId)
        {
            this.transport = transport ?? throw new ArgumentNullException(nameof(transport));
            if (schema == null)
            {
                throw new ArgumentNullException(nameof(schema));
            }
            this.config = (config ?? new SessionConfig()).Clone();
            this.peerId = peerId;
            packetTransport = transport.Kind == TransportKind.Message;
            frameSource = packetTransport
                ? (IFrameSource)new MessageFrameSource(transport)
                : new StreamFrameSource(transport);
            session = new Session(role, schema, this.config, now);

            var opening = session.Start();
            if (opening.FrameLength > 0)
            {
                WriteControl(session.OutBuffer, opening.FrameLength);
            }
        }

        public static FomoxaConnection Connect(
            ITransport transport, Schema schema, SessionConfig config, TimeSpan now) =>
            new FomoxaConnection(transport, schema, config, FomoxaRole.Client, now);

        public static FomoxaConnection Connect(ITransport transport, Schema schema, SessionConfig config) =>
            Connect(transport, schema, config, MonotonicClock.Now);

        public SessionState State => session.State;

        public bool IsReady => session.State == SessionState.Ready;

        public bool IsClosed => session.IsClosed;

        public ulong PeerId => peerId;

        public ITransport Transport => transport;

        public bool IsCongested => outboxUsed > 0 || pendingPackets.Count > 0;

        public IReadOnlyList<FomoxaEvent> Tick(TimeSpan now)
        {
            events.Clear();
            eventPayloadsUsed = 0;

            if (!connectedEmitted)
            {
                connectedEmitted = true;
                events.Add(FomoxaEvent.Connected(peerId));
            }

            if (!transportDead && IsCongested)
            {
                Flush();
            }

            if (!transportDead && !session.IsClosed)
            {
                Drain(now);
            }

            if (!transportDead && !session.IsClosed)
            {
                Apply(session.Tick(now), default);
            }

            if (transportDead && !session.IsClosed)
            {
                Apply(
                    overloaded ? session.ReportOverloaded() : session.TransportClosed(gracefulDeath),
                    default);
            }

            return events;
        }

        public IReadOnlyList<FomoxaEvent> Tick() => Tick(MonotonicClock.Now);

        public SendStatus Send(uint messageId, ReadOnlySpan<byte> payload)
        {
            if (session.IsClosed || transportDead)
            {
                return SendStatus.Closed;
            }
            if (session.State != SessionState.Ready)
            {
                return SendStatus.NotReady;
            }
            if (payload.Length > FomoxaWire.MaxMessagePayload)
            {
                return SendStatus.TooLarge;
            }

            int frameSize = FrameLayout.DataFrameSize(payload.Length);
            if (scratch.Length < frameSize)
            {
                scratch = new byte[frameSize];
            }
            FrameLayout.WriteDataFrame(scratch.AsSpan(0, frameSize), messageId, payload);

            if (IsCongested)
            {
                return SendStatus.Congested;
            }

            var outcome = transport.Send(scratch.AsSpan(0, frameSize));
            switch (outcome.Signal)
            {
                case TransportSignal.Ok:
                    return SendStatus.Sent;

                case TransportSignal.WouldBlock:
                    Hold(scratch, frameSize, true);
                    return transportDead ? SendStatus.Closed : SendStatus.Sent;

                case TransportSignal.Partial when !packetTransport:
                    StoreOutbox(scratch, outcome.Count, frameSize - outcome.Count);
                    return SendStatus.Sent;

                case TransportSignal.TooLarge:
                    return SendStatus.TooLarge;

                case TransportSignal.Closed:
                    Kill(true);
                    return SendStatus.Closed;

                default:
                    Kill(false);
                    return SendStatus.Closed;
            }
        }

        public SendStatus Send(uint messageId, byte[] payload) =>
            Send(messageId, new ReadOnlySpan<byte>(payload ?? Array.Empty<byte>()));

        public void Close()
        {
            if (session.IsClosed)
            {
                return;
            }
            session.CloseLocal();
            outboxUsed = 0;
            pendingPackets.Clear();
            pendingCountedBytes = 0;
            transportDead = true;
            transport.CloseGracefully();
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            transport.Dispose();
        }

        public void ShrinkToFit()
        {
            frameSource.ShrinkToFit();
            session.ShrinkToFit();
            events.TrimExcess();

            if (eventPayloadsUsed == 0 && eventPayloads.Length > DefaultEventPayloadCapacity)
            {
                eventPayloads = new byte[DefaultEventPayloadCapacity];
            }
            if (outboxUsed == 0 && outbox.Length > 0)
            {
                outbox = Array.Empty<byte>();
            }
            if (scratch.Length > 0)
            {
                scratch = Array.Empty<byte>();
            }
            if (partBuffer.Length > 0)
            {
                partBuffer = Array.Empty<byte>();
            }
            if (pendingPackets.Count == 0)
            {
                pendingPackets.TrimExcess();
            }
            assembler.ShrinkToFit();
        }

        private void Drain(TimeSpan now)
        {
            int budget = config.MaxFramesPerTick;
            var frame = default(SourcedFrame);

            while (budget > 0)
            {
                var status = frameSource.Next(ref frame);
                switch (status)
                {
                    case FrameSourceStatus.Frame:
                        Apply(Handle(frame, now), frame);
                        budget--;
                        if (session.IsClosed || transportDead)
                        {
                            return;
                        }
                        continue;

                    case FrameSourceStatus.Empty:
                        return;

                    case FrameSourceStatus.Closed:
                        Kill(true);
                        return;

                    default:
                        Kill(false);
                        return;
                }
            }
        }

        private SessionAction Handle(SourcedFrame frame, TimeSpan now)
        {
            if (frame.Type == FrameType.HandshakePart)
            {
                return HandlePart(frame.Payload, now);
            }
            if (frame.Type == FrameType.Handshake && assembler.IsPending)
            {
                assembler.Reset();
                return session.HandleFrame(FrameType.Handshake, ReadOnlySpan<byte>.Empty, now);
            }
            return session.HandleFrame(frame.Type, frame.Payload, now);
        }

        private SessionAction HandlePart(ReadOnlySpan<byte> part, TimeSpan now)
        {
            if (session.State != SessionState.Handshaking)
            {
                return SessionAction.Nothing;
            }

            switch (assembler.Add(part))
            {
                case AssemblyStatus.Pending:
                    return session.HandleFrame(FrameType.HandshakePart, ReadOnlySpan<byte>.Empty, now);

                case AssemblyStatus.Complete:
                {
                    var action = session.HandleFrame(FrameType.Handshake, assembler.Content, now);
                    assembler.Reset();
                    return action;
                }

                default:
                    assembler.Reset();
                    return session.HandleFrame(FrameType.Handshake, ReadOnlySpan<byte>.Empty, now);
            }
        }

        private void Apply(SessionAction action, SourcedFrame frame)
        {
            if (action.FrameLength > 0)
            {
                WriteControl(session.OutBuffer, action.FrameLength);
            }

            switch (action.Emit)
            {
                case SessionEmit.None:
                    break;

                case SessionEmit.Ready:
                    events.Add(FomoxaEvent.Ready(peerId));
                    break;

                case SessionEmit.Message:
                {
                    int offset = StorePayload(frame.Payload);
                    events.Add(FomoxaEvent.Message(peerId, frame.MessageId, eventPayloads, offset, frame.Length));
                    break;
                }

                case SessionEmit.Ping:
                    events.Add(FomoxaEvent.Ping(peerId));
                    break;

                case SessionEmit.Pong:
                    events.Add(FomoxaEvent.Pong(peerId));
                    break;

                case SessionEmit.Disconnected:
                    events.Add(FomoxaEvent.Disconnected(peerId, action.Reason));
                    break;

                case SessionEmit.HandshakeFailed:
                    events.Add(FomoxaEvent.HandshakeFailed(peerId, action.Failure));
                    transportDead = true;
                    transport.CloseGracefully();
                    break;
            }
        }

        private int StorePayload(ReadOnlySpan<byte> payload)
        {
            if (eventPayloads.Length - eventPayloadsUsed < payload.Length)
            {
                int capacity = Math.Max(eventPayloads.Length, 1);
                long needed = (long)eventPayloadsUsed + payload.Length;
                while (capacity < needed)
                {
                    capacity *= 2;
                }
                var grown = new byte[capacity];
                Buffer.BlockCopy(eventPayloads, 0, grown, 0, eventPayloadsUsed);
                eventPayloads = grown;
            }

            int offset = eventPayloadsUsed;
            payload.CopyTo(eventPayloads.AsSpan(offset));
            eventPayloadsUsed += payload.Length;
            return offset;
        }

        private void WriteControl(byte[] bytes, int length)
        {
            if (packetTransport
                && length > FomoxaWire.PacketHandshakeBudget
                && bytes[0] == (byte)FrameType.Handshake)
            {
                WriteHandshakeParts(new ReadOnlySpan<byte>(
                    bytes, FomoxaWire.HandshakeFrameHeaderSize, length - FomoxaWire.HandshakeFrameHeaderSize));
                return;
            }
            WriteFrame(bytes, length, true);
        }

        private void WriteHandshakeParts(ReadOnlySpan<byte> content)
        {
            int count = (content.Length + FomoxaWire.HandshakePartContent - 1) / FomoxaWire.HandshakePartContent;
            if (partBuffer.Length < FomoxaWire.PacketHandshakeBudget)
            {
                partBuffer = new byte[FomoxaWire.PacketHandshakeBudget];
            }

            for (int index = 0; index < count && !transportDead; index++)
            {
                int offset = index * FomoxaWire.HandshakePartContent;
                int partLength = Math.Min(FomoxaWire.HandshakePartContent, content.Length - offset);
                FrameLayout.WriteHandshakePart(partBuffer, index, count, content.Slice(offset, partLength));
                WriteFrame(partBuffer, FrameLayout.HandshakePartFrameSize(partLength), false);
            }
        }

        private void WriteFrame(byte[] bytes, int length, bool counted)
        {
            if (transportDead)
            {
                return;
            }
            if (IsCongested)
            {
                Hold(bytes, length, counted);
                return;
            }

            var outcome = transport.Send(new ReadOnlySpan<byte>(bytes, 0, length));
            switch (outcome.Signal)
            {
                case TransportSignal.Ok:
                    break;

                case TransportSignal.WouldBlock:
                    Hold(bytes, length, counted);
                    break;

                case TransportSignal.Partial when !packetTransport:
                    StoreOutbox(bytes, outcome.Count, length - outcome.Count);
                    break;

                case TransportSignal.TooLarge:
                    break;

                case TransportSignal.Closed:
                    Kill(true);
                    break;

                default:
                    Kill(false);
                    break;
            }
        }

        private void Flush()
        {
            if (packetTransport)
            {
                FlushPackets();
                return;
            }

            while (outboxUsed > 0)
            {
                var outcome = transport.Send(new ReadOnlySpan<byte>(outbox, 0, outboxUsed));
                switch (outcome.Signal)
                {
                    case TransportSignal.Ok:
                        outboxUsed = 0;
                        return;

                    case TransportSignal.WouldBlock:
                        return;

                    case TransportSignal.Partial:
                        Buffer.BlockCopy(outbox, outcome.Count, outbox, 0, outboxUsed - outcome.Count);
                        outboxUsed -= outcome.Count;
                        break;

                    case TransportSignal.TooLarge:
                        outboxUsed = 0;
                        return;

                    case TransportSignal.Closed:
                        Kill(true);
                        return;

                    default:
                        Kill(false);
                        return;
                }
            }
        }

        private void FlushPackets()
        {
            while (pendingPackets.Count > 0)
            {
                var packet = pendingPackets.Peek();
                var outcome = transport.Send(packet.Bytes);
                switch (outcome.Signal)
                {
                    case TransportSignal.Ok:
                    case TransportSignal.TooLarge:
                        pendingPackets.Dequeue();
                        if (packet.Counted)
                        {
                            pendingCountedBytes -= packet.Bytes.Length;
                        }
                        break;

                    case TransportSignal.WouldBlock:
                        return;

                    case TransportSignal.Closed:
                        Kill(true);
                        return;

                    default:
                        Kill(false);
                        return;
                }
            }
        }

        private void Hold(byte[] bytes, int length, bool counted)
        {
            if (!packetTransport)
            {
                if (outboxUsed > 0)
                {
                    AppendOutbox(bytes, length);
                }
                else
                {
                    StoreOutbox(bytes, 0, length);
                }
                return;
            }

            if (counted && pendingCountedBytes + length > MaxOutboxBytes)
            {
                overloaded = true;
                Kill(false);
                return;
            }
            pendingPackets.Enqueue(new PendingPacket(new ReadOnlySpan<byte>(bytes, 0, length).ToArray(), counted));
            if (counted)
            {
                pendingCountedBytes += length;
            }
        }

        private void StoreOutbox(byte[] bytes, int offset, int length)
        {
            if (outbox.Length < length)
            {
                outbox = new byte[length];
            }
            Buffer.BlockCopy(bytes, offset, outbox, 0, length);
            outboxUsed = length;
        }

        private void AppendOutbox(byte[] bytes, int length)
        {
            // The protocol caps how many control frames can be owed at once:
            // one probe per silence window, one REPLY per POLL, at most one
            // query round per session. Passing this means an assumption broke,
            // so the session ends rather than the buffer growing (02 §8).
            if (outboxUsed + length > MaxOutboxBytes)
            {
                overloaded = true;
                Kill(false);
                return;
            }

            if (outbox.Length - outboxUsed < length)
            {
                var grown = new byte[outboxUsed + length];
                Buffer.BlockCopy(outbox, 0, grown, 0, outboxUsed);
                outbox = grown;
            }
            Buffer.BlockCopy(bytes, 0, outbox, outboxUsed, length);
            outboxUsed += length;
        }

        private readonly struct PendingPacket
        {
            public PendingPacket(byte[] bytes, bool counted)
            {
                Bytes = bytes;
                Counted = counted;
            }

            public byte[] Bytes { get; }

            public bool Counted { get; }
        }

        private void Kill(bool graceful)
        {
            if (transportDead)
            {
                return;
            }
            transportDead = true;
            gracefulDeath = graceful;
        }
    }
}
