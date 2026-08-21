using System;

namespace Cyclone.Net
{
    public enum CycloneEventKind
    {
        Connected,
        Ready,
        HandshakeFailed,
        Message,
        Ping,
        Pong,
        Disconnected,
    }

    public enum HandshakeFailure
    {
        VersionMismatch = 1,
        SchemaConflict = 2,
        Corrupt = 3,
        Timeout = 255,
    }

    public enum DisconnectReason
    {
        PeerClosed,
        TransportError,
        Timeout,
    }

    public readonly struct CycloneEvent
    {
        private readonly byte[]? payloadBuffer;
        private readonly int payloadOffset;
        private readonly int payloadLength;

        private CycloneEvent(
            CycloneEventKind kind,
            ulong peerId,
            uint messageId,
            byte[]? payloadBuffer,
            int payloadOffset,
            int payloadLength,
            HandshakeFailure failure,
            DisconnectReason reason)
        {
            Kind = kind;
            PeerId = peerId;
            MessageId = messageId;
            this.payloadBuffer = payloadBuffer;
            this.payloadOffset = payloadOffset;
            this.payloadLength = payloadLength;
            Failure = failure;
            Reason = reason;
        }

        public CycloneEventKind Kind { get; }

        public ulong PeerId { get; }

        public uint MessageId { get; }

        public HandshakeFailure Failure { get; }

        public DisconnectReason Reason { get; }

        public ReadOnlyMemory<byte> Payload =>
            payloadBuffer == null
                ? ReadOnlyMemory<byte>.Empty
                : new ReadOnlyMemory<byte>(payloadBuffer, payloadOffset, payloadLength);

        public byte[] CopyPayload() => Payload.ToArray();

        internal static CycloneEvent Connected(ulong peerId) =>
            new CycloneEvent(CycloneEventKind.Connected, peerId, 0, null, 0, 0, default, default);

        internal static CycloneEvent Ready(ulong peerId) =>
            new CycloneEvent(CycloneEventKind.Ready, peerId, 0, null, 0, 0, default, default);

        internal static CycloneEvent HandshakeFailed(ulong peerId, HandshakeFailure failure) =>
            new CycloneEvent(CycloneEventKind.HandshakeFailed, peerId, 0, null, 0, 0, failure, default);

        internal static CycloneEvent Message(
            ulong peerId, uint messageId, byte[] buffer, int offset, int length) =>
            new CycloneEvent(CycloneEventKind.Message, peerId, messageId, buffer, offset, length, default, default);

        internal static CycloneEvent Ping(ulong peerId) =>
            new CycloneEvent(CycloneEventKind.Ping, peerId, 0, null, 0, 0, default, default);

        internal static CycloneEvent Pong(ulong peerId) =>
            new CycloneEvent(CycloneEventKind.Pong, peerId, 0, null, 0, 0, default, default);

        internal static CycloneEvent Disconnected(ulong peerId, DisconnectReason reason) =>
            new CycloneEvent(CycloneEventKind.Disconnected, peerId, 0, null, 0, 0, default, reason);

        internal CycloneEvent WithPeer(ulong peerId) =>
            new CycloneEvent(Kind, peerId, MessageId, payloadBuffer, payloadOffset, payloadLength, Failure, Reason);
    }
}
