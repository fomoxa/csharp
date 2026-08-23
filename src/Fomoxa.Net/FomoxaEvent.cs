using System;

namespace Fomoxa.Net
{
    public enum FomoxaEventKind
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

    public readonly struct FomoxaEvent
    {
        private readonly byte[]? payloadBuffer;
        private readonly int payloadOffset;
        private readonly int payloadLength;

        private FomoxaEvent(
            FomoxaEventKind kind,
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

        public FomoxaEventKind Kind { get; }

        public ulong PeerId { get; }

        public uint MessageId { get; }

        public HandshakeFailure Failure { get; }

        public DisconnectReason Reason { get; }

        public ReadOnlyMemory<byte> Payload =>
            payloadBuffer == null
                ? ReadOnlyMemory<byte>.Empty
                : new ReadOnlyMemory<byte>(payloadBuffer, payloadOffset, payloadLength);

        public byte[] CopyPayload() => Payload.ToArray();

        internal static FomoxaEvent Connected(ulong peerId) =>
            new FomoxaEvent(FomoxaEventKind.Connected, peerId, 0, null, 0, 0, default, default);

        internal static FomoxaEvent Ready(ulong peerId) =>
            new FomoxaEvent(FomoxaEventKind.Ready, peerId, 0, null, 0, 0, default, default);

        internal static FomoxaEvent HandshakeFailed(ulong peerId, HandshakeFailure failure) =>
            new FomoxaEvent(FomoxaEventKind.HandshakeFailed, peerId, 0, null, 0, 0, failure, default);

        internal static FomoxaEvent Message(
            ulong peerId, uint messageId, byte[] buffer, int offset, int length) =>
            new FomoxaEvent(FomoxaEventKind.Message, peerId, messageId, buffer, offset, length, default, default);

        internal static FomoxaEvent Ping(ulong peerId) =>
            new FomoxaEvent(FomoxaEventKind.Ping, peerId, 0, null, 0, 0, default, default);

        internal static FomoxaEvent Pong(ulong peerId) =>
            new FomoxaEvent(FomoxaEventKind.Pong, peerId, 0, null, 0, 0, default, default);

        internal static FomoxaEvent Disconnected(ulong peerId, DisconnectReason reason) =>
            new FomoxaEvent(FomoxaEventKind.Disconnected, peerId, 0, null, 0, 0, default, reason);

        internal FomoxaEvent WithPeer(ulong peerId) =>
            new FomoxaEvent(Kind, peerId, MessageId, payloadBuffer, payloadOffset, payloadLength, Failure, Reason);
    }
}
