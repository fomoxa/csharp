using System;
using System.Diagnostics;

namespace Cyclone.Net
{
    public static class MonotonicClock
    {
        private static readonly Stopwatch Running = Stopwatch.StartNew();

        public static TimeSpan Now => Running.Elapsed;
    }
}
