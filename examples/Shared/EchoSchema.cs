using Fomoxa.Net;

namespace Fomoxa.Net.Examples
{
    public static class EchoSchema
    {
        public const ulong SchemaFingerprint = 0x9C41A0B27D3E5518;

        public const uint EchoMessageId = 0x00000001;

        public const ulong EchoFingerprint = 0x51E0C7A43B92660F;

        public static readonly ulong[] EchoPrefixes = { EchoFingerprint };

        public static Schema Build() =>
            new Schema(
                SchemaFingerprint,
                new[] { new MessageSchema(EchoMessageId, EchoFingerprint, EchoPrefixes) });
    }
}
