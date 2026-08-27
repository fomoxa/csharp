using System.Collections.Generic;
using Fomoxa.Net;

namespace Fomoxa.Reliability
{
    public static class ReliabilitySchema
    {
        public const uint EnvelopeMessageId = 0xF0000001;
        public const uint AckMessageId = 0xF0000002;

        private const ulong EnvelopeFingerprint = 0x52454C5F454E5631UL;
        private const ulong AckFingerprint = 0x52454C5F41434B31UL;

        private static readonly ulong[] EnvelopePrefixes = { EnvelopeFingerprint };
        private static readonly ulong[] AckPrefixes = { AckFingerprint };

        /// Adds the two control messages this library needs (envelope + ack) to an
        /// application schema and returns the combined schema to hand to
        /// FomoxaConnection/FomoxaServer. Message ids 0xF0000001/0xF0000002 are
        /// reserved by this library; an application schema that already declares
        /// one of them fails fast with a SchemaException from the Schema constructor.
        public static Schema Combine(Schema appSchema)
        {
            var messages = new List<MessageSchema>(appSchema.Messages)
            {
                new MessageSchema(EnvelopeMessageId, EnvelopeFingerprint, EnvelopePrefixes),
                new MessageSchema(AckMessageId, AckFingerprint, AckPrefixes),
            };

            // Not a real fomoxa-fingerprint/2 hash of the merged set - just a cheap
            // combination so two peers built from the same appSchema still hit the
            // handshake's exact-match fast path. A mismatch always falls back to the
            // real per-message prefix comparison, which stays correct either way.
            ulong combinedFingerprint = appSchema.Fingerprint ^ EnvelopeFingerprint ^ AckFingerprint;

            return new Schema(combinedFingerprint, messages);
        }
    }
}
