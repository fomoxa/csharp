using System;
using System.Collections.Generic;

namespace Fomoxa.Reliability.Tests
{
    public static class Program
    {
        public static int Main()
        {
            var cases = new (string Name, Action Body)[]
            {
                ("unreliable delivers without ack", ReliabilityTests.UnreliableDeliversWithoutAck),
                ("reliable survives one dropped envelope", ReliabilityTests.ReliableSurvivesOneDroppedEnvelope),
                ("a congested reliable send is not lost", ReliabilityTests.CongestedReliableSendIsNotLost),
                ("pending sends are capped", ReliabilityTests.PendingSendsAreCapped),
                ("ack replies are capped per message", ReliabilityTests.AckRepliesAreCappedPerMessage),
                ("a reliable send before ready is delivered once ready", ReliabilityTests.NotReadySendIsRegisteredAndEventuallyDelivered),
                ("broadcast reliable tracks each peer independently", ReliabilityTests.BroadcastReliableTracksEachPeerIndependently),
                ("disconnect removes server-side peer state", ReliabilityTests.DisconnectRemovesServerSidePeerState),
                ("metrics track send, resend and deliver", ReliabilityTests.MetricsTrackSendResendAndDeliver),
                ("metrics track rejections and failures", ReliabilityTests.MetricsTrackRejectionsAndFailures),
                ("duplicate envelope is not delivered twice", ReliabilityTests.DuplicateEnvelopeIsNotDeliveredTwice),
                ("gives up after max attempts", ReliabilityTests.GivesUpAfterMaxAttempts),
            };

            int passed = 0;
            var failures = new List<string>();

            foreach (var testCase in cases)
            {
                try
                {
                    testCase.Body();
                    passed++;
                    Console.WriteLine($"   ok    {testCase.Name}");
                }
                catch (Exception error)
                {
                    string detail = error is AssertionException
                        ? error.Message
                        : $"{error.GetType().Name}: {error.Message}";
                    failures.Add($"{testCase.Name}: {detail}");
                    Console.WriteLine($"   FAIL  {testCase.Name}");
                    Console.WriteLine($"         {detail}");
                }
            }

            Console.WriteLine();
            Console.WriteLine($"{passed} passed, {failures.Count} failed, {cases.Length} total");
            return failures.Count > 0 ? 1 : 0;
        }
    }
}
