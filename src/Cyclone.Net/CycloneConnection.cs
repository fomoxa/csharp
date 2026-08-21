using System;
using System.Collections.Generic;
using Cyclone.Net.Framing;
using Cyclone.Net.Transports;

namespace Cyclone.Net
{
    public enum SendStatus
    {
        Sent,
        NotReady,
        Congested,
        TooLarge,
        Closed,
    }

    public sealed class CycloneConnection : IDisposable
    {
        private readonly ITransport transport;
        private readonly IFrameSource frameSource;
        private readonly Session session;
        private readonly SessionConfig config;
        private readonly ulong peerId;
        private readonly List<CycloneEvent> events = new List<CycloneEvent>();

        private byte[] eventPayloads = new byte[4096];
        private int eventPayloadsUsed;
        private byte[] outbox = new byte[0];
        private int outboxUsed;
        private byte[] scratch = new byte[0];
        private bool transportDead;
        private bool gracefulDeath;
        private bool connectedEmitted;
        private bool released;

        public CycloneConnection(
            ITransport transport, Schema schema, SessionConfig config, CycloneRole role, TimeSpan now)
            : this(transport, schema, config, role, now, 0)
        {
        }

        internal CycloneConnection(
            ITransport transport,
            Schema schema,
            SessionConfig config,
            CycloneRole role,
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
            frameSource = transport.Kind == TransportKind.Stream
                ? (IFrameSource)new StreamFrameSource(transport)
                : new MessageFrameSource(transport);
            session = new Session(role, schema, this.config, now);

            var opening = session.Start();
            if (opening.FrameLength > 0)
            {
                WriteControl(session.OutBuffer, opening.FrameLength);
            }
        }

        public static CycloneConnection Connect(
            ITransport transport, Schema schema, SessionConfig config, TimeSpan now) =>
            new CycloneConnection(transport, schema, config, CycloneRole.Client, now);

        public static CycloneConnection Connect(ITransport transport, Schema schema, SessionConfig config) =>
            Connect(transport, schema, config, MonotonicClock.Now);

        public SessionState State => session.State;

        public bool IsReady => session.State == SessionState.Ready;

        public bool IsClosed => session.IsClosed;

        public ulong PeerId => peerId;

        public ITransport Transport => transport;

        public IReadOnlyList<CycloneEvent> Tick(TimeSpan now)
        {
            events.Clear();
            eventPayloadsUsed = 0;

            if (!connectedEmitted)
            {
                connectedEmitted = true;
                events.Add(CycloneEvent.Connected(peerId));
            }

            if (!transportDead && outboxUsed > 0)
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
                Apply(session.TransportClosed(gracefulDeath), default);
            }

            return events;
        }

        public IReadOnlyList<CycloneEvent> Tick() => Tick(MonotonicClock.Now);

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
            if (payload.Length > CycloneWire.MaxMessagePayload)
            {
                return SendStatus.TooLarge;
            }

            int frameSize = FrameLayout.DataFrameSize(payload.Length);
            if (scratch.Length < frameSize)
            {
                scratch = new byte[frameSize];
            }
            FrameLayout.WriteDataFrame(scratch.AsSpan(0, frameSize), messageId, payload);

            if (outboxUsed > 0)
            {
                return SendStatus.Congested;
            }

            var outcome = transport.Send(scratch.AsSpan(0, frameSize));
            switch (outcome.Signal)
            {
                case TransportSignal.Ok:
                    return SendStatus.Sent;

                case TransportSignal.WouldBlock:
                    StoreOutbox(scratch, frameSize);
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
                        Apply(session.HandleFrame(frame.Type, frame.Payload, now), frame);
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
                    events.Add(CycloneEvent.Ready(peerId));
                    break;

                case SessionEmit.Message:
                {
                    int offset = StorePayload(frame.Payload);
                    events.Add(CycloneEvent.Message(peerId, frame.MessageId, eventPayloads, offset, frame.Length));
                    break;
                }

                case SessionEmit.Ping:
                    events.Add(CycloneEvent.Ping(peerId));
                    break;

                case SessionEmit.Pong:
                    events.Add(CycloneEvent.Pong(peerId));
                    break;

                case SessionEmit.Disconnected:
                    events.Add(CycloneEvent.Disconnected(peerId, action.Reason));
                    break;

                case SessionEmit.HandshakeFailed:
                    events.Add(CycloneEvent.HandshakeFailed(peerId, action.Failure));
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
            if (transportDead)
            {
                return;
            }
            if (outboxUsed > 0)
            {
                AppendOutbox(bytes, length);
                return;
            }

            var outcome = transport.Send(new ReadOnlySpan<byte>(bytes, 0, length));
            switch (outcome.Signal)
            {
                case TransportSignal.Ok:
                    break;

                case TransportSignal.WouldBlock:
                    StoreOutbox(bytes, length);
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

        private void StoreOutbox(byte[] bytes, int length)
        {
            if (outbox.Length < length)
            {
                outbox = new byte[length];
            }
            Buffer.BlockCopy(bytes, 0, outbox, 0, length);
            outboxUsed = length;
        }

        private void AppendOutbox(byte[] bytes, int length)
        {
            if (outbox.Length - outboxUsed < length)
            {
                var grown = new byte[outboxUsed + length];
                Buffer.BlockCopy(outbox, 0, grown, 0, outboxUsed);
                outbox = grown;
            }
            Buffer.BlockCopy(bytes, 0, outbox, outboxUsed, length);
            outboxUsed += length;
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
