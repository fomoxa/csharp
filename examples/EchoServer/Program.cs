using System;
using System.Net;
using System.Text;
using System.Threading;
using Fomoxa.Net;
using Fomoxa.Net.Transports;

namespace Fomoxa.Net.Examples.EchoServer
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            int port = args.Length > 0 ? int.Parse(args[0]) : 7777;
            bool udp = args.Length > 1 && args[1] == "udp";

            var schema = EchoSchema.Build();
            var config = new SessionConfig();

            IListenerTransport listener = udp
                ? new UdpServerTransport(new IPEndPoint(IPAddress.Loopback, port))
                : new TcpListenerTransport(new IPEndPoint(IPAddress.Loopback, port));

            using var server = new FomoxaServer(listener, schema, config);
            Console.WriteLine($"echo server listening on {(udp ? "udp" : "tcp")} 127.0.0.1:{port}");

            var stop = new ManualResetEventSlim(false);
            Console.CancelKeyPress += (_, cancel) =>
            {
                cancel.Cancel = true;
                stop.Set();
            };

            while (!stop.IsSet)
            {
                foreach (var raised in server.Tick(MonotonicClock.Now))
                {
                    switch (raised.Kind)
                    {
                        case FomoxaEventKind.Connected:
                            Console.WriteLine($"peer {raised.PeerId} arrived");
                            break;

                        case FomoxaEventKind.Ready:
                            Console.WriteLine($"peer {raised.PeerId} agreed on the schema");
                            break;

                        case FomoxaEventKind.HandshakeFailed:
                            Console.WriteLine($"peer {raised.PeerId} refused: {raised.Failure}");
                            break;

                        case FomoxaEventKind.Message:
                            var text = Encoding.UTF8.GetString(raised.Payload.Span);
                            Console.WriteLine($"peer {raised.PeerId} said: {text}");
                            server.Send(raised.PeerId, EchoSchema.EchoMessageId, raised.Payload.Span);
                            break;

                        case FomoxaEventKind.Disconnected:
                            Console.WriteLine($"peer {raised.PeerId} left: {raised.Reason}");
                            break;
                    }
                }

                Thread.Sleep(16);
            }
        }
    }
}
