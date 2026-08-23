namespace Fomoxa.Net
{
    public static class FomoxaWire
    {
        public const uint ProtocolVersion = 2;

        public const byte DataMagicFirst = 0x43;
        public const byte DataMagicSecond = 0x59;

        public const int MaxMessagePayload = 16 * 1024 * 1024;
        public const int DataFrameHeaderSize = 11;
        public const int MaxDataFrameSize = DataFrameHeaderSize + MaxMessagePayload;

        public const int HandshakeFrameHeaderSize = 5;
        public const int MaxHandshakePayload = 1024 * 1024;
        public const int MaxHandshakeFrameSize = HandshakeFrameHeaderSize + MaxHandshakePayload;

        public const int MaxHelloMessages = 1_000_000;
        public const int MaxQueryItems = 1_000_000;

        public const int HelloHeaderSize = 16;
        public const int HelloEntrySize = 14;

        public const int QueryHeaderSize = 5;
        public const int QueryEntrySize = 6;

        public const int QueryReplyHeaderSize = 4;
        public const int QueryReplyEntrySize = 12;

        public const byte QueryVerdictByte = 4;
    }
}
