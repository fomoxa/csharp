using System;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Tests
{
    public static class HeartbeatTests
    {
        private const string Group = "heartbeat";

        public static void Register()
        {
            TestRegistry.Add(Group, "a link carrying traffic never produces a probe", TrafficSuppressesProbes);
            TestRegistry.Add(Group, "silence produces exactly one probe, not one per tick", OneProbePerSilence);
            TestRegistry.Add(Group, "an answer puts the session back to normal", AnswerClearsProbe);
            TestRegistry.Add(Group, "any frame, not only an answer, clears the probe", AnyFrameClearsProbe);
            TestRegistry.Add(Group, "no answer inside the deadline declares the peer dead", NoAnswerIsDeath);
            TestRegistry.Add(Group, "a probe is always sent before a peer is declared dead", ProbeBeforeDeath);
            TestRegistry.Add(Group, "the client runs no heartbeat until it is ready", NoClientHeartbeatWhileHandshaking);
            TestRegistry.Add(Group, "the server probes a handshaking peer on the handshake window", ServerProbesDuringHandshake);
            TestRegistry.Add(Group, "becoming ready narrows the server's silence window only", ReadyNarrowsTheWindow);
        }

        private static void TrafficSuppressesProbes()
        {
            var side = ReadyClient();
            var now = TimeSpan.Zero;

            for (int tick = 0; tick < 30; tick++)
            {
                now += TimeSpan.FromSeconds(1);
                side.Transport.Deliver(Wire.DataFrame(1, new byte[] { 7 }));
                side.Connection.Tick(now);
                Check.Equal(0, side.Transport.Outgoing.Count, $"nothing was sent at {now.TotalSeconds}s");
            }
        }

        private static void OneProbePerSilence()
        {
            var side = ReadyClient();

            side.Connection.Tick(TimeSpan.FromSeconds(4));
            Check.Equal(0, side.Transport.Outgoing.Count, "no probe before the window closes");

            side.Connection.Tick(TimeSpan.FromSeconds(5));
            Check.Bytes(new byte[] { (byte)FrameType.Ping }, side.Transport.TakeOutgoing(), "one probe");

            side.Connection.Tick(TimeSpan.FromSeconds(6));
            side.Connection.Tick(TimeSpan.FromSeconds(7));
            Check.Equal(0, side.Transport.Outgoing.Count, "the probe is not repeated every tick");
        }

        private static void AnswerClearsProbe()
        {
            var side = ReadyClient();

            side.Connection.Tick(TimeSpan.FromSeconds(5));
            side.Transport.TakeOutgoing();

            side.Transport.Deliver(Wire.Pong());
            var events = side.Connection.Tick(TimeSpan.FromSeconds(6));
            Check.True(Events.Has(events, FomoxaEventKind.Pong), "answer event");

            side.Connection.Tick(TimeSpan.FromSeconds(10));
            Check.Equal(0, side.Transport.Outgoing.Count, "the clock restarted from the answer");

            side.Connection.Tick(TimeSpan.FromSeconds(11));
            Check.Bytes(new byte[] { (byte)FrameType.Ping }, side.Transport.TakeOutgoing(), "a fresh probe");
        }

        private static void AnyFrameClearsProbe()
        {
            var side = ReadyClient();

            side.Connection.Tick(TimeSpan.FromSeconds(5));
            side.Transport.TakeOutgoing();

            side.Transport.Deliver(Wire.DataFrame(3, new byte[] { 1 }));
            var events = side.Connection.Tick(TimeSpan.FromSeconds(6));
            Check.True(Events.Has(events, FomoxaEventKind.Message), "the data arrived");

            var later = side.Connection.Tick(TimeSpan.FromSeconds(20));
            Check.False(Events.Has(later, FomoxaEventKind.Disconnected), "the peer was never declared dead");
        }

        private static void NoAnswerIsDeath()
        {
            var side = ReadyClient();

            side.Connection.Tick(TimeSpan.FromSeconds(5));
            side.Transport.TakeOutgoing();

            Check.Equal(0, side.Connection.Tick(TimeSpan.FromSeconds(19)).Count, "still alive one second short");

            var events = side.Connection.Tick(TimeSpan.FromSeconds(20));
            var disconnect = Events.First(events, FomoxaEventKind.Disconnected);
            Check.Equal(DisconnectReason.Timeout, disconnect.Reason, "disconnect reason");
            Check.Equal(SessionState.Closed, side.Connection.State, "session state");
        }

        private static void ProbeBeforeDeath()
        {
            var side = ReadyClient();

            var events = side.Connection.Tick(TimeSpan.FromSeconds(5));
            Check.False(Events.Has(events, FomoxaEventKind.Disconnected), "the window closing kills nobody");
            Check.Bytes(new byte[] { (byte)FrameType.Ping }, side.Transport.TakeOutgoing(), "a probe went first");
        }

        private static void NoClientHeartbeatWhileHandshaking()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, TimeSpan.Zero);
            transport.TakeOutgoing();

            connection.Tick(TimeSpan.FromSeconds(4));
            Check.Equal(0, transport.Outgoing.Count, "a handshaking client sends no probe");
        }

        private static void ServerProbesDuringHandshake()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Server, TimeSpan.Zero);

            connection.Tick(TimeSpan.FromSeconds(4));
            Check.Equal(0, transport.Outgoing.Count, "nothing before the handshake window closes");

            connection.Tick(TimeSpan.FromSeconds(5));
            Check.Bytes(new byte[] { (byte)FrameType.Ping }, transport.TakeOutgoing(), "the server probes");
        }

        private static void ReadyNarrowsTheWindow()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(0xF00D, Schemas.Message(1, 10));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Server, TimeSpan.Zero);

            transport.Deliver(Wire.Hello(2, 0xF00D));
            connection.Tick(TimeSpan.FromSeconds(1));
            transport.TakeOutgoing();
            Check.Equal(SessionState.Ready, connection.State, "peer is ready");

            connection.Tick(TimeSpan.FromSeconds(5));
            Check.Equal(0, transport.Outgoing.Count, "four seconds of silence is not enough");

            connection.Tick(TimeSpan.FromSeconds(6));
            Check.Bytes(new byte[] { (byte)FrameType.Ping }, transport.TakeOutgoing(), "five seconds is");
        }

        private readonly struct Side
        {
            public Side(FomoxaConnection connection, FakeTransport transport)
            {
                Connection = connection;
                Transport = transport;
            }

            public FomoxaConnection Connection { get; }

            public FakeTransport Transport { get; }
        }

        private static Side ReadyClient()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, TimeSpan.Zero);
            transport.TakeOutgoing();
            transport.Deliver(Wire.Verdict(0));
            connection.Tick(TimeSpan.Zero);
            Check.Equal(SessionState.Ready, connection.State, "client is ready");
            return new Side(connection, transport);
        }
    }
}
