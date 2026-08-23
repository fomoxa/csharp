using System;

namespace Fomoxa.Net
{
    public enum FomoxaRole
    {
        Client,
        Server,
    }

    public enum SessionState
    {
        Handshaking,
        Ready,
        Closed,
    }

    public sealed class SessionConfig
    {
        public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);

        public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(15);

        public int MaxFramesPerTick { get; set; } = 64;

        public SessionConfig Clone()
        {
            return new SessionConfig
            {
                HandshakeTimeout = HandshakeTimeout,
                HeartbeatInterval = HeartbeatInterval,
                HeartbeatTimeout = HeartbeatTimeout,
                MaxFramesPerTick = MaxFramesPerTick,
            };
        }
    }
}
