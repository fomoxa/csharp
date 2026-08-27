using System;
using Fomoxa.Net;

namespace Fomoxa.Reliability
{
    public enum ReliableEventKind
    {
        Connected,
        Ready,
        HandshakeFailed,
        Message,
        Disconnected,
        Delivered,
        DeliveryFailed,
    }

    public readonly struct ReliableEvent
    {
        private ReliableEvent(
            ReliableEventKind kind,
            ulong peerId,
            uint messageId,
            ReadOnlyMemory<byte> payload,
            bool wasReliable,
            HandshakeFailure failure,
            DisconnectReason reason)
        {
            Kind = kind;
            PeerId = peerId;
            MessageId = messageId;
            Payload = payload;
            WasReliable = wasReliable;
            Failure = failure;
            Reason = reason;
        }

        public ReliableEventKind Kind { get; }

        public ulong PeerId { get; }

        /// For Message: the app's own message id (the envelope, if any, is already unwrapped).
        /// For Delivered/DeliveryFailed: the message id originally passed to SendReliable.
        public uint MessageId { get; }

        /// Same lifetime rule as FomoxaEvent.Payload: valid only until this
        /// channel's next Tick call. Copy it if you need it to outlive that.
        public ReadOnlyMemory<byte> Payload { get; }

        public bool WasReliable { get; }

        public HandshakeFailure Failure { get; }

        public DisconnectReason Reason { get; }

        public byte[] CopyPayload() => Payload.ToArray();

        internal static ReliableEvent Connected(ulong peerId) =>
            new ReliableEvent(
                ReliableEventKind.Connected, peerId, 0, ReadOnlyMemory<byte>.Empty, false, default, default);

        internal static ReliableEvent Ready(ulong peerId) =>
            new ReliableEvent(
                ReliableEventKind.Ready, peerId, 0, ReadOnlyMemory<byte>.Empty, false, default, default);

        internal static ReliableEvent HandshakeFailed(ulong peerId, HandshakeFailure failure) =>
            new ReliableEvent(
                ReliableEventKind.HandshakeFailed, peerId, 0, ReadOnlyMemory<byte>.Empty, false, failure, default);

        internal static ReliableEvent Message(
            ulong peerId, uint messageId, ReadOnlyMemory<byte> payload, bool wasReliable) =>
            new ReliableEvent(
                ReliableEventKind.Message, peerId, messageId, payload, wasReliable, default, default);

        internal static ReliableEvent Disconnected(ulong peerId, DisconnectReason reason) =>
            new ReliableEvent(
                ReliableEventKind.Disconnected, peerId, 0, ReadOnlyMemory<byte>.Empty, false, default, reason);

        internal static ReliableEvent Delivered(ulong peerId, uint messageId) =>
            new ReliableEvent(
                ReliableEventKind.Delivered, peerId, messageId, ReadOnlyMemory<byte>.Empty, true, default, default);

        internal static ReliableEvent DeliveryFailed(ulong peerId, uint messageId) =>
            new ReliableEvent(
                ReliableEventKind.DeliveryFailed, peerId, messageId, ReadOnlyMemory<byte>.Empty, true, default,
                default);
    }
}
