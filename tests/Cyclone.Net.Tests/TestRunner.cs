using System;
using System.Collections.Generic;

namespace Cyclone.Net.Tests
{
    public sealed class AssertionException : Exception
    {
        public AssertionException(string message) : base(message)
        {
        }
    }

    public static class Check
    {
        public static void True(bool condition, string what)
        {
            if (!condition)
            {
                throw new AssertionException(what);
            }
        }

        public static void False(bool condition, string what) => True(!condition, what);

        public static void Equal<T>(T expected, T actual, string what)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
            {
                throw new AssertionException($"{what}: expected {expected}, got {actual}");
            }
        }

        public static void Bytes(byte[] expected, ReadOnlySpan<byte> actual, string what)
        {
            if (expected.Length != actual.Length)
            {
                throw new AssertionException(
                    $"{what}: expected {expected.Length} bytes [{Hex(expected)}], " +
                    $"got {actual.Length} [{Hex(actual)}]");
            }
            for (int index = 0; index < expected.Length; index++)
            {
                if (expected[index] != actual[index])
                {
                    throw new AssertionException(
                        $"{what}: byte {index} expected {expected[index]:X2}, got {actual[index]:X2} " +
                        $"(expected [{Hex(expected)}], got [{Hex(actual)}])");
                }
            }
        }

        public static string Hex(ReadOnlySpan<byte> bytes)
        {
            var text = new System.Text.StringBuilder(bytes.Length * 3);
            for (int index = 0; index < bytes.Length; index++)
            {
                if (index > 0)
                {
                    text.Append(' ');
                }
                text.Append(bytes[index].ToString("X2"));
            }
            return text.ToString();
        }
    }

    public static class TestRegistry
    {
        private static readonly List<(string Group, string Name, Action Body)> Cases =
            new List<(string, string, Action)>();

        public static void Add(string group, string name, Action body) => Cases.Add((group, name, body));

        public static IReadOnlyList<(string Group, string Name, Action Body)> All => Cases;
    }

    public static class Program
    {
        public static int Main()
        {
            FrameTests.Register();
            HandshakeTests.Register();
            HeartbeatTests.Register();
            FlowTests.Register();
            InteropTests.Register();
            SocketTests.Register();

            int passed = 0;
            var failures = new List<string>();
            string? currentGroup = null;

            foreach (var testCase in TestRegistry.All)
            {
                if (testCase.Group != currentGroup)
                {
                    currentGroup = testCase.Group;
                    Console.WriteLine();
                    Console.WriteLine($"── {currentGroup}");
                }

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
                    failures.Add($"{testCase.Group} / {testCase.Name}: {detail}");
                    Console.WriteLine($"   FAIL  {testCase.Name}");
                    Console.WriteLine($"         {detail}");
                }
            }

            Console.WriteLine();
            Console.WriteLine($"{passed} passed, {failures.Count} failed, {TestRegistry.All.Count} total");
            if (failures.Count > 0)
            {
                Console.WriteLine();
                foreach (var failure in failures)
                {
                    Console.WriteLine($"  ✗ {failure}");
                }
                return 1;
            }
            return 0;
        }
    }
}
