using System;
using System.Diagnostics;

namespace Fomoxa.Net
{
    public static class MonotonicClock
    {
        private static readonly Stopwatch Running = Stopwatch.StartNew();

        public static TimeSpan Now => Running.Elapsed;
    }
}
