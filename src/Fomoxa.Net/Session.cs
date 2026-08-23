using System;
using System.Collections.Generic;
using Fomoxa.Net.Framing;

namespace Fomoxa.Net
{
    internal enum SessionEmit
    {
        None,
        Ready,
        HandshakeFailed,
        Message,
        Ping,
        Pong,
        Disconnected,
    }

    internal readonly struct SessionAction
    {
        private SessionAction(
            int frameLength, SessionEmit emit, HandshakeFailure failure, DisconnectReason reason)
        {
            FrameLength = frameLength;
            Emit = emit;
            Failure = failure;
            Reason = reason;
        }

        public int FrameLength { get; }

        public SessionEmit Emit { get; }

        public HandshakeFailure Failure { get; }

        public DisconnectReason Reason { get; }

        public static readonly SessionAction Nothing =
            new SessionAction(0, SessionEmit.None, default, default);

        public static SessionAction Frame(int frameLength) =>
            new SessionAction(frameLength, SessionEmit.None, default, default);

        public static SessionAction Event(SessionEmit emit) =>
            new SessionAction(0, emit, default, default);

        public static SessionAction FrameAndEvent(int frameLength, SessionEmit emit) =>
            new SessionAction(frameLength, emit, default, default);

        public static SessionAction Failed(int frameLength, HandshakeFailure failure) =>
            new SessionAction(frameLength, SessionEmit.HandshakeFailed, failure, default);

        public static SessionAction Gone(DisconnectReason reason) =>
            new SessionAction(0, SessionEmit.Disconnected, default, reason);
    }

    internal sealed class Session
    {
        private readonly FomoxaRole role;
        private readonly Schema schema;
        private readonly SessionConfig config;
        private readonly TimeSpan createdAt;
        private readonly List<AskItem> asks = new List<AskItem>();

        private byte[] outBuffer = new byte[64];
        private TimeSpan lastActivity;
        private TimeSpan probeSentAt;
        private bool probing;
        private bool terminalEmitted;
        private bool helloHandled;
        private bool awaitingQueryReply;
        private bool queryHandled;

        public Session(FomoxaRole role, Schema schema, SessionConfig config, TimeSpan now)
        {
            this.role = role;
            this.schema = schema;
            this.config = config;
            createdAt = now;
            lastActivity = now;
            State = SessionState.Handshaking;
        }

        public SessionState State { get; private set; }

        public byte[] OutBuffer => outBuffer;

        public bool IsClosed => State == SessionState.Closed;

        public SessionAction Start()
        {
            if (role != FomoxaRole.Client)
            {
                return SessionAction.Nothing;
            }

            int payloadSize = HandshakeCodec.HelloSize(schema);
            int frameSize = FrameLayout.HandshakeFrameSize(payloadSize);
            EnsureOut(frameSize);
            FrameLayout.WriteHandshakeHeader(outBuffer, payloadSize);
            HandshakeCodec.WriteHello(
                outBuffer.AsSpan(FomoxaWire.HandshakeFrameHeaderSize, payloadSize), schema);
            return SessionAction.Frame(frameSize);
        }

        public SessionAction HandleFrame(FrameType type, ReadOnlySpan<byte> payload, TimeSpan now)
        {
            if (State == SessionState.Closed)
            {
                return SessionAction.Nothing;
            }

            lastActivity = now;
            probing = false;

            switch (type)
            {
                case FrameType.Ping:
                    EnsureOut(1);
                    FrameLayout.WritePong(outBuffer);
                    return State == SessionState.Ready
                        ? SessionAction.FrameAndEvent(1, SessionEmit.Ping)
                        : SessionAction.Frame(1);

                case FrameType.Pong:
                    return State == SessionState.Ready
                        ? SessionAction.Event(SessionEmit.Pong)
                        : SessionAction.Nothing;

                case FrameType.Data:
                    return State == SessionState.Ready
                        ? SessionAction.Event(SessionEmit.Message)
                        : SessionAction.Nothing;

                default:
                    return role == FomoxaRole.Client
                        ? ClientHandshake(payload)
                        : ServerHandshake(payload);
            }
        }

        public SessionAction Tick(TimeSpan now)
        {
            if (State == SessionState.Closed)
            {
                return SessionAction.Nothing;
            }

            if (role == FomoxaRole.Client
                && State == SessionState.Handshaking
                && now - createdAt >= config.HandshakeTimeout)
            {
                return Fail(0, HandshakeFailure.Timeout);
            }

            bool heartbeatRuns = role == FomoxaRole.Server || State == SessionState.Ready;
            if (!heartbeatRuns)
            {
                return SessionAction.Nothing;
            }

            if (probing)
            {
                if (now - probeSentAt >= config.HeartbeatTimeout)
                {
                    State = SessionState.Closed;
                    terminalEmitted = true;
                    return SessionAction.Gone(DisconnectReason.Timeout);
                }
                return SessionAction.Nothing;
            }

            TimeSpan silenceWindow = role == FomoxaRole.Server && State == SessionState.Handshaking
                ? config.HandshakeTimeout
                : config.HeartbeatInterval;

            if (now - lastActivity >= silenceWindow)
            {
                probing = true;
                probeSentAt = now;
                EnsureOut(1);
                FrameLayout.WritePing(outBuffer);
                return SessionAction.Frame(1);
            }

            return SessionAction.Nothing;
        }

        public SessionAction TransportClosed(bool graceful)
        {
            if (State == SessionState.Closed)
            {
                return SessionAction.Nothing;
            }
            State = SessionState.Closed;
            if (terminalEmitted)
            {
                return SessionAction.Nothing;
            }
            terminalEmitted = true;
            return SessionAction.Gone(
                graceful ? DisconnectReason.PeerClosed : DisconnectReason.TransportError);
        }

        /// <summary>
        /// Ends the session because the peer stopped reading and the pending
        /// queue reached its ceiling. The reason is the same one a heartbeat
        /// expiry raises: a peer that stops reading and a peer that stops
        /// answering are both a peer that is not keeping up. The transport did
        /// not break, so reporting a transport error would be untrue.
        /// </summary>
        public SessionAction ReportOverloaded()
        {
            if (terminalEmitted)
            {
                return SessionAction.Nothing;
            }
            State = SessionState.Closed;
            terminalEmitted = true;
            return SessionAction.Gone(DisconnectReason.Timeout);
        }

        public void CloseLocal()
        {
            State = SessionState.Closed;
            terminalEmitted = true;
        }

        private SessionAction ClientHandshake(ReadOnlySpan<byte> payload)
        {
            if (State != SessionState.Handshaking)
            {
                return SessionAction.Nothing;
            }
            if (payload.Length == 0)
            {
                return Fail(0, HandshakeFailure.Corrupt);
            }

            if (payload[0] == FomoxaWire.QueryVerdictByte)
            {
                return ClientQuery(payload);
            }

            if (payload.Length != 1 || payload[0] > 3)
            {
                return Fail(0, HandshakeFailure.Corrupt);
            }
            if (payload[0] == 0)
            {
                State = SessionState.Ready;
                return SessionAction.Event(SessionEmit.Ready);
            }
            return Fail(0, (HandshakeFailure)payload[0]);
        }

        private SessionAction ClientQuery(ReadOnlySpan<byte> payload)
        {
            if (queryHandled)
            {
                return Fail(0, HandshakeFailure.Corrupt);
            }
            queryHandled = true;

            if (!HandshakeCodec.TryQueryCount(payload, out int count))
            {
                return Fail(0, HandshakeFailure.Corrupt);
            }

            int replySize = HandshakeCodec.QueryReplySize(count);
            if (replySize > FomoxaWire.MaxHandshakePayload)
            {
                return Fail(0, HandshakeFailure.Corrupt);
            }
            int frameSize = FrameLayout.HandshakeFrameSize(replySize);
            EnsureOut(frameSize);

            var reply = outBuffer.AsSpan(FomoxaWire.HandshakeFrameHeaderSize, replySize);
            HandshakeCodec.WriteQueryReplyHeader(reply, count);

            for (int index = 0; index < count; index++)
            {
                HandshakeCodec.QueryEntry(payload, index, out uint id, out ushort fieldCount);
                var known = schema.Message(id);
                if (known == null || !known.TryPrefix(fieldCount, out ulong fingerprint))
                {
                    return Fail(0, HandshakeFailure.Corrupt);
                }
                HandshakeCodec.WriteQueryReplyEntry(reply, index, id, fingerprint);
            }

            FrameLayout.WriteHandshakeHeader(outBuffer, replySize);
            return SessionAction.Frame(frameSize);
        }

        private SessionAction ServerHandshake(ReadOnlySpan<byte> payload)
        {
            if (State != SessionState.Handshaking)
            {
                return SessionAction.Nothing;
            }
            if (!helloHandled)
            {
                helloHandled = true;
                return ServerHello(payload);
            }
            if (awaitingQueryReply)
            {
                awaitingQueryReply = false;
                return ServerQueryReply(payload);
            }
            return SessionAction.Nothing;
        }

        private SessionAction ServerHello(ReadOnlySpan<byte> payload)
        {
            if (!HandshakeCodec.TryHelloCount(payload, out int count))
            {
                return Verdict(3);
            }
            if (HandshakeCodec.HelloVersion(payload) != FomoxaWire.ProtocolVersion)
            {
                return Verdict(1);
            }
            if (HandshakeCodec.HelloSchemaFingerprint(payload) == schema.Fingerprint)
            {
                return Verdict(0);
            }

            asks.Clear();
            for (int index = 0; index < count; index++)
            {
                HandshakeCodec.HelloEntry(
                    payload, index, out uint id, out ushort peerFieldCount, out ulong peerFingerprint);

                var known = schema.Message(id);
                if (known == null)
                {
                    continue;
                }
                if (peerFingerprint == known.Fingerprint)
                {
                    continue;
                }

                ushort localFieldCount = known.FieldCount;
                if (peerFieldCount == 0 || localFieldCount == 0)
                {
                    continue;
                }
                if (peerFieldCount == localFieldCount)
                {
                    return Verdict(2);
                }
                if (peerFieldCount < localFieldCount)
                {
                    if (!known.TryPrefix(peerFieldCount, out ulong localPrefix) || localPrefix != peerFingerprint)
                    {
                        return Verdict(2);
                    }
                    continue;
                }
                asks.Add(new AskItem(id, localFieldCount));
            }

            if (asks.Count == 0)
            {
                return Verdict(0);
            }

            int querySize = HandshakeCodec.QuerySize(asks.Count);
            int frameSize = FrameLayout.HandshakeFrameSize(querySize);
            EnsureOut(frameSize);
            FrameLayout.WriteHandshakeHeader(outBuffer, querySize);
            HandshakeCodec.WriteQuery(
                outBuffer.AsSpan(FomoxaWire.HandshakeFrameHeaderSize, querySize), asks);
            awaitingQueryReply = true;
            return SessionAction.Frame(frameSize);
        }

        private SessionAction ServerQueryReply(ReadOnlySpan<byte> payload)
        {
            if (!HandshakeCodec.TryQueryReplyCount(payload, out int count) || count != asks.Count)
            {
                return Verdict(3);
            }

            for (int index = 0; index < count; index++)
            {
                HandshakeCodec.QueryReplyEntry(payload, index, out uint id, out ulong peerPrefix);
                if (id != asks[index].Id)
                {
                    return Verdict(3);
                }
                var known = schema.Message(id);
                if (known == null || !known.TryPrefix(asks[index].FieldCount, out ulong localPrefix))
                {
                    return Verdict(3);
                }
                if (localPrefix != peerPrefix)
                {
                    return Verdict(2);
                }
            }

            return Verdict(0);
        }

        private SessionAction Verdict(byte verdict)
        {
            int frameSize = FrameLayout.HandshakeFrameSize(1);
            EnsureOut(frameSize);
            FrameLayout.WriteHandshakeHeader(outBuffer, 1);
            outBuffer[FomoxaWire.HandshakeFrameHeaderSize] = verdict;

            if (verdict == 0)
            {
                State = SessionState.Ready;
                return SessionAction.FrameAndEvent(frameSize, SessionEmit.Ready);
            }
            return Fail(frameSize, (HandshakeFailure)verdict);
        }

        private SessionAction Fail(int frameLength, HandshakeFailure failure)
        {
            State = SessionState.Closed;
            terminalEmitted = true;
            return SessionAction.Failed(frameLength, failure);
        }

        private void EnsureOut(int size)
        {
            if (outBuffer.Length < size)
            {
                outBuffer = new byte[size];
            }
        }
    }
}
