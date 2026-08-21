using System;

namespace Cyclone.Net.Transports
{
    public enum TransportKind
    {
        Stream,
        Message,
    }

    public enum TransportSignal
    {
        Ok,
        WouldBlock,
        Closed,
        Error,
        TooLarge,
        NeedCapacity,
    }

    public readonly struct SendOutcome
    {
        private SendOutcome(TransportSignal signal)
        {
            Signal = signal;
        }

        public TransportSignal Signal { get; }

        public static SendOutcome Ok => new SendOutcome(TransportSignal.Ok);

        public static SendOutcome WouldBlock => new SendOutcome(TransportSignal.WouldBlock);

        public static SendOutcome Closed => new SendOutcome(TransportSignal.Closed);

        public static SendOutcome Error => new SendOutcome(TransportSignal.Error);

        public static SendOutcome TooLarge => new SendOutcome(TransportSignal.TooLarge);
    }

    public readonly struct ReceiveOutcome
    {
        private ReceiveOutcome(TransportSignal signal, int count)
        {
            Signal = signal;
            Count = count;
        }

        public TransportSignal Signal { get; }

        public int Count { get; }

        public static ReceiveOutcome Ok(int byteCount) => new ReceiveOutcome(TransportSignal.Ok, byteCount);

        public static ReceiveOutcome NeedCapacity(int requiredBytes) =>
            new ReceiveOutcome(TransportSignal.NeedCapacity, requiredBytes);

        public static ReceiveOutcome WouldBlock => new ReceiveOutcome(TransportSignal.WouldBlock, 0);

        public static ReceiveOutcome Closed => new ReceiveOutcome(TransportSignal.Closed, 0);

        public static ReceiveOutcome Error => new ReceiveOutcome(TransportSignal.Error, 0);
    }

    public interface ITransport : IDisposable
    {
        TransportKind Kind { get; }

        SendOutcome Send(ReadOnlySpan<byte> bytes);

        ReceiveOutcome Receive(Span<byte> buffer);

        void CloseGracefully();
    }

    public enum AcceptStatus
    {
        Accepted,
        Progress,
        Pending,
        Error,
    }

    public readonly struct AcceptOutcome
    {
        private AcceptOutcome(AcceptStatus status, ITransport? transport)
        {
            Status = status;
            Transport = transport;
        }

        public AcceptStatus Status { get; }

        public ITransport? Transport { get; }

        public static AcceptOutcome Accepted(ITransport transport) =>
            new AcceptOutcome(AcceptStatus.Accepted, transport);

        public static AcceptOutcome Progress => new AcceptOutcome(AcceptStatus.Progress, null);

        public static AcceptOutcome Pending => new AcceptOutcome(AcceptStatus.Pending, null);

        public static AcceptOutcome Error => new AcceptOutcome(AcceptStatus.Error, null);
    }

    public interface IListenerTransport : IDisposable
    {
        AcceptOutcome Accept();
    }
}
