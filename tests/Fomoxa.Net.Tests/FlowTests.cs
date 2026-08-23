using System;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Tests
{
    public static class FlowTests
    {
        private const string Group = "core loop and transport contract";

        public static void Register()
        {
            TestRegistry.Add(Group, "CONNECTED is the very first event and is raised once", ConnectedIsFirst);
            TestRegistry.Add(Group, "a blocked link parks the frame and sends it once, later", BlockedFrameIsParked);
            TestRegistry.Add(Group, "sending again while parked is refused instead of queued", SecondSendIsRefused);
            TestRegistry.Add(Group, "a parked frame goes out before anything else", ParkedFrameGoesFirst);
            TestRegistry.Add(Group, "a frame past the link's ceiling fails the send but not the session", TooLargeKeepsSessionAlive);
            TestRegistry.Add(Group, "a packet larger than the buffer is kept, not lost", NeedCapacityKeepsPacket);
            TestRegistry.Add(Group, "a burst stops at the tick budget and resumes next tick", BudgetStopsTheDrain);
            TestRegistry.Add(Group, "a clean transport close reports the peer closing", CleanCloseReason);
            TestRegistry.Add(Group, "a broken transport reports a transport error", BrokenTransportReason);
            TestRegistry.Add(Group, "a frame violation on a byte stream ends the session", StreamViolationIsFatal);
            TestRegistry.Add(Group, "a blocked link plus a peer polling every tick stops at the ceiling", PendingQueueStopsAtItsCeiling);
            TestRegistry.Add(Group, "a failed handshake followed by a dead link raises one end event", OneEndEventOnly);
            TestRegistry.Add(Group, "closing locally raises no event and closes the link politely", LocalCloseIsSilent);
            TestRegistry.Add(Group, "two messages in one tick both carry their own payload", PayloadsSurviveTheTick);
        }

        private static void ConnectedIsFirst()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, TimeSpan.Zero);
            transport.TakeOutgoing();
            transport.Deliver(Wire.Verdict(0));

            var events = connection.Tick(TimeSpan.Zero);
            Check.True(events.Count >= 2, "the first tick carries both events");
            Check.Equal(FomoxaEventKind.Connected, events[0].Kind, "first event");
            Check.Equal(FomoxaEventKind.Ready, events[1].Kind, "second event");

            var later = connection.Tick(TimeSpan.Zero);
            Check.False(Events.Has(later, FomoxaEventKind.Connected), "CONNECTED does not repeat");
        }

        private static void BlockedFrameIsParked()
        {
            var side = ReadyClient(out var transport);
            transport.BlockSends = true;

            Check.Equal(SendStatus.Sent, side.Connection.Send(9, new byte[] { 1, 2, 3 }), "the send is accepted");
            Check.Equal(0, transport.Outgoing.Count, "nothing reached the link");

            transport.BlockSends = false;
            side.Connection.Tick(TimeSpan.Zero);

            Check.Bytes(
                Wire.DataFrame(9, new byte[] { 1, 2, 3 }),
                transport.TakeOutgoing(),
                "the parked frame went out whole");
            Check.Equal(0, transport.Outgoing.Count, "and exactly once");
        }

        private static void SecondSendIsRefused()
        {
            var side = ReadyClient(out var transport);
            transport.BlockSends = true;

            Check.Equal(SendStatus.Sent, side.Connection.Send(1, new byte[] { 1 }), "first send parks");
            Check.Equal(SendStatus.Congested, side.Connection.Send(2, new byte[] { 2 }), "second send is refused");

            transport.BlockSends = false;
            side.Connection.Tick(TimeSpan.Zero);
            Check.Bytes(Wire.DataFrame(1, new byte[] { 1 }), transport.TakeOutgoing(), "only the first frame exists");
            Check.Equal(0, transport.Outgoing.Count, "the refused message was never queued");
        }

        private static void ParkedFrameGoesFirst()
        {
            var side = ReadyClient(out var transport);
            transport.BlockSends = true;
            side.Connection.Send(1, new byte[] { 0xAA });

            transport.Deliver(Wire.Ping());
            transport.BlockSends = false;
            side.Connection.Tick(TimeSpan.Zero);

            Check.Bytes(Wire.DataFrame(1, new byte[] { 0xAA }), transport.TakeOutgoing(), "the parked frame is first");
            Check.Bytes(new byte[] { (byte)FrameType.Pong }, transport.TakeOutgoing(), "the answer follows it");
        }

        private static void TooLargeKeepsSessionAlive()
        {
            var side = ReadyClient(out var transport);
            transport.SendCeiling = 20;

            Check.Equal(SendStatus.TooLarge, side.Connection.Send(1, new byte[100]), "the send fails");
            Check.Equal(SessionState.Ready, side.Connection.State, "the session is untouched");

            int attempts = transport.SendAttempts;
            side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(attempts, transport.SendAttempts, "nothing was retried");

            transport.SendCeiling = int.MaxValue;
            Check.Equal(SendStatus.Sent, side.Connection.Send(1, new byte[] { 1 }), "later sends still work");
        }

        private static void NeedCapacityKeepsPacket()
        {
            var side = ReadyClient(out var transport);
            var payload = new byte[4000];
            for (int index = 0; index < payload.Length; index++)
            {
                payload[index] = (byte)(index & 0xFF);
            }

            transport.Deliver(Wire.DataFrame(5, payload));
            var events = side.Connection.Tick(TimeSpan.Zero);

            var message = Events.First(events, FomoxaEventKind.Message);
            Check.Equal(5u, message.MessageId, "message id");
            Check.Bytes(payload, message.Payload.Span, "the packet came through untouched");
        }

        private static void BudgetStopsTheDrain()
        {
            var side = ReadyClient(out var transport);
            for (int index = 0; index < 12; index++)
            {
                transport.Deliver(Wire.DataFrame((uint)index, new byte[] { (byte)index }));
            }

            var first = side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(8, Events.Count(first, FomoxaEventKind.Message), "the tick budget held");

            var second = side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(4, Events.Count(second, FomoxaEventKind.Message), "the rest arrived next tick");
        }

        private static void CleanCloseReason()
        {
            var side = ReadyClient(out var transport);
            transport.ReportClosed = true;

            var events = side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(
                DisconnectReason.PeerClosed,
                Events.First(events, FomoxaEventKind.Disconnected).Reason,
                "disconnect reason");
        }

        private static void BrokenTransportReason()
        {
            var side = ReadyClient(out var transport);
            transport.ReportError = true;

            var events = side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(
                DisconnectReason.TransportError,
                Events.First(events, FomoxaEventKind.Disconnected).Reason,
                "disconnect reason");
        }

        private static void StreamViolationIsFatal()
        {
            var transport = new FakeTransport(TransportKind.Stream);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, TimeSpan.Zero);
            transport.TakeOutgoing();

            transport.Deliver(new byte[] { 0x7F });
            var events = connection.Tick(TimeSpan.Zero);
            Check.True(Events.Has(events, FomoxaEventKind.Disconnected), "the session ended");
            Check.Equal(SessionState.Closed, connection.State, "session state");
        }

        private static void OneEndEventOnly()
        {
            var transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10, 20));
            using var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Server, TimeSpan.Zero);

            transport.Deliver(Wire.Hello(2, 999, (1, 2, 21)));
            var first = connection.Tick(TimeSpan.Zero);
            Check.Equal(1, Events.Count(first, FomoxaEventKind.HandshakeFailed), "the handshake failed");
            Check.Equal(0, Events.Count(first, FomoxaEventKind.Disconnected), "and nothing else ended it");
            Check.True(transport.GracefullyClosed, "the link was closed politely");

            transport.ReportError = true;
            var second = connection.Tick(TimeSpan.Zero);
            Check.Equal(0, second.Count, "a dead link afterwards adds no second end event");
        }

        private static void LocalCloseIsSilent()
        {
            var side = ReadyClient(out var transport);
            side.Connection.Close();

            Check.True(transport.GracefullyClosed, "the link was closed politely");
            Check.Equal(SessionState.Closed, side.Connection.State, "session state");
            Check.Equal(0, side.Connection.Tick(TimeSpan.Zero).Count, "no event follows a local close");
        }

        private static void PayloadsSurviveTheTick()
        {
            var side = ReadyClient(out var transport);
            transport.Deliver(Wire.DataFrame(1, new byte[] { 1, 1, 1 }));
            transport.Deliver(Wire.DataFrame(2, new byte[] { 2, 2 }));

            var events = side.Connection.Tick(TimeSpan.Zero);
            Check.Equal(2, Events.Count(events, FomoxaEventKind.Message), "both messages arrived");

            var first = default(FomoxaEvent);
            var second = default(FomoxaEvent);
            int seen = 0;
            foreach (var item in events)
            {
                if (item.Kind != FomoxaEventKind.Message)
                {
                    continue;
                }
                if (seen == 0)
                {
                    first = item;
                }
                else
                {
                    second = item;
                }
                seen++;
            }

            Check.Bytes(new byte[] { 1, 1, 1 }, first.Payload.Span, "first payload");
            Check.Bytes(new byte[] { 2, 2 }, second.Payload.Span, "second payload");
        }

        private readonly struct Side
        {
            public Side(FomoxaConnection connection)
            {
                Connection = connection;
            }

            public FomoxaConnection Connection { get; }
        }

        // 02 §8: the pending queue must have a ceiling. A peer that pings every
        // tick while never reading keeps our silence clock alive, so heartbeat
        // never ends the session; only the ceiling does.
        private static void PendingQueueStopsAtItsCeiling()
        {
            var side = ReadyClient(out var transport);
            transport.BlockSends = true;

            var events = new System.Collections.Generic.List<FomoxaEvent>();
            for (var tick = 0; tick < 200_000 && !Events.Has(events, FomoxaEventKind.Disconnected); tick++)
            {
                transport.Deliver(Wire.Ping());
                events.AddRange(side.Connection.Tick(TimeSpan.FromMilliseconds(tick)));
            }

            Check.True(
                Events.Has(events, FomoxaEventKind.Disconnected),
                "the ceiling ends the session instead of letting the queue grow");
            Check.Equal(
                1,
                Events.Count(events, FomoxaEventKind.Disconnected),
                "exactly one termination event");
            Check.Equal(
                DisconnectReason.Timeout,
                Events.First(events, FomoxaEventKind.Disconnected).Reason,
                "a peer that stops reading is not keeping up, not a broken link");
        }

        private static Side ReadyClient(out FakeTransport transport)
        {
            transport = new FakeTransport(TransportKind.Message);
            var schema = Schemas.Of(1, Schemas.Message(1, 10));
            var connection = new FomoxaConnection(
                transport, schema, Schemas.Config(), FomoxaRole.Client, TimeSpan.Zero);
            transport.TakeOutgoing();
            transport.Deliver(Wire.Verdict(0));
            connection.Tick(TimeSpan.Zero);
            Check.Equal(SessionState.Ready, connection.State, "client is ready");
            return new Side(connection);
        }
    }
}
