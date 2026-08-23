using System;
using System.Net;
using System.Text;
using System.Threading;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Examples.EchoClient
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            int port = args.Length > 0 ? int.Parse(args[0]) : 7777;
            bool udp = args.Length > 1 && args[1] == "udp";

            var schema = EchoSchema.Build();
            var config = new SessionConfig();
            var remote = new IPEndPoint(IPAddress.Loopback, port);

            ITransport transport = udp ? UdpTransport.Connect(remote) : TcpTransport.Connect(remote);
            using var connection = FomoxaConnection.Connect(transport, schema, config);
            Console.WriteLine($"connecting to {(udp ? "udp" : "tcp")} {remote}");

            int sent = 0;
            var nextSend = MonotonicClock.Now;

            while (!connection.IsClosed && sent < 5)
            {
                foreach (var raised in connection.Tick(MonotonicClock.Now))
                {
                    switch (raised.Kind)
                    {
                        case FomoxaEventKind.Ready:
                            Console.WriteLine("the server agreed on the schema");
                            break;

                        case FomoxaEventKind.HandshakeFailed:
                            Console.WriteLine($"handshake refused: {raised.Failure}");
                            return;

                        case FomoxaEventKind.Message:
                            Console.WriteLine($"echo: {Encoding.UTF8.GetString(raised.Payload.Span)}");
                            break;

                        case FomoxaEventKind.Disconnected:
                            Console.WriteLine($"disconnected: {raised.Reason}");
                            return;
                    }
                }

                if (connection.IsReady && MonotonicClock.Now >= nextSend)
                {
                    sent++;
                    var payload = Encoding.UTF8.GetBytes($"hello {sent}");
                    var status = connection.Send(EchoSchema.EchoMessageId, payload);
                    if (status != SendStatus.Sent)
                    {
                        Console.WriteLine($"send refused: {status}");
                    }
                    nextSend = MonotonicClock.Now + TimeSpan.FromMilliseconds(500);
                }

                Thread.Sleep(16);
            }

            for (int drain = 0; drain < 60; drain++)
            {
                foreach (var raised in connection.Tick(MonotonicClock.Now))
                {
                    if (raised.Kind == FomoxaEventKind.Message)
                    {
                        Console.WriteLine($"echo: {Encoding.UTF8.GetString(raised.Payload.Span)}");
                    }
                }
                Thread.Sleep(16);
            }

            connection.Close();
        }
    }
}
