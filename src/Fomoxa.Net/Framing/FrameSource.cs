using System;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Framing
{
    internal enum FrameSourceStatus
    {
        Frame,
        Empty,
        Closed,
        Error,
        Corrupt,
    }

    internal struct SourcedFrame
    {
        public FrameType Type;
        public uint MessageId;
        public byte[] Buffer;
        public int Offset;
        public int Length;

        public ReadOnlySpan<byte> Payload =>
            Buffer == null ? ReadOnlySpan<byte>.Empty : Buffer.AsSpan(Offset, Length);
    }

    internal interface IFrameSource
    {
        FrameSourceStatus Next(ref SourcedFrame frame);
        void ShrinkToFit();
    }

    internal sealed class StreamFrameSource : IFrameSource
    {
        private const int InitialReadSize = 8192;

        private readonly ITransport transport;
        private readonly StreamFrameDecoder decoder = new StreamFrameDecoder();
        private byte[] readBuffer = new byte[InitialReadSize];

        public StreamFrameSource(ITransport transport)
        {
            this.transport = transport;
        }

        public FrameSourceStatus Next(ref SourcedFrame frame)
        {
            while (true)
            {
                var scan = decoder.TryRead(out frame.Type, out frame.MessageId, out int offset, out int length);
                if (scan == FrameScan.Ok)
                {
                    frame.Buffer = decoder.Buffer;
                    frame.Offset = offset;
                    frame.Length = length;
                    return FrameSourceStatus.Frame;
                }
                if (scan == FrameScan.Corrupt)
                {
                    return FrameSourceStatus.Corrupt;
                }

                var outcome = transport.Receive(readBuffer);
                switch (outcome.Signal)
                {
                    case TransportSignal.Ok:
                        if (outcome.Count <= 0)
                        {
                            return FrameSourceStatus.Empty;
                        }
                        decoder.Push(readBuffer.AsSpan(0, outcome.Count));
                        continue;

                    case TransportSignal.NeedCapacity:
                        if (outcome.Count <= readBuffer.Length)
                        {
                            return FrameSourceStatus.Error;
                        }
                        readBuffer = new byte[outcome.Count];
                        continue;

                    case TransportSignal.WouldBlock:
                        return FrameSourceStatus.Empty;

                    case TransportSignal.Closed:
                        return FrameSourceStatus.Closed;

                    default:
                        return FrameSourceStatus.Error;
                }
            }
        }

        public void ShrinkToFit()
        {
            if (readBuffer.Length > InitialReadSize)
            {
                readBuffer = new byte[InitialReadSize];
            }
            decoder.ShrinkToFit();
        }
    }

    internal sealed class MessageFrameSource : IFrameSource
    {
        private const int InitialPacketSize = 2048;

        private readonly ITransport transport;
        private byte[] packetBuffer = new byte[InitialPacketSize];

        public MessageFrameSource(ITransport transport)
        {
            this.transport = transport;
        }

        public FrameSourceStatus Next(ref SourcedFrame frame)
        {
            while (true)
            {
                var outcome = transport.Receive(packetBuffer);
                switch (outcome.Signal)
                {
                    case TransportSignal.Ok:
                    {
                        var packet = packetBuffer.AsSpan(0, outcome.Count);
                        var scan = FrameLayout.Scan(
                            packet, out frame.Type, out frame.MessageId, out int headerSize, out int length);
                        if (scan != FrameScan.Ok || headerSize + length != outcome.Count)
                        {
                            continue;
                        }
                        frame.Buffer = packetBuffer;
                        frame.Offset = headerSize;
                        frame.Length = length;
                        return FrameSourceStatus.Frame;
                    }

                    case TransportSignal.NeedCapacity:
                        if (outcome.Count <= packetBuffer.Length)
                        {
                            return FrameSourceStatus.Error;
                        }
                        packetBuffer = new byte[outcome.Count];
                        continue;

                    case TransportSignal.WouldBlock:
                        return FrameSourceStatus.Empty;

                    case TransportSignal.Closed:
                        return FrameSourceStatus.Closed;

                    default:
                        return FrameSourceStatus.Error;
                }
            }
        }

        public void ShrinkToFit()
        {
            if (packetBuffer.Length > InitialPacketSize)
            {
                packetBuffer = new byte[InitialPacketSize];
            }
        }
    }
}
