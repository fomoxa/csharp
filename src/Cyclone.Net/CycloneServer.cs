using System;
using System.Collections.Generic;
using Cyclone.Net.Transports;

namespace Cyclone.Net
{
    public sealed class CycloneServer : IDisposable
    {
        private readonly IListenerTransport listener;
        private readonly Schema schema;
        private readonly SessionConfig config;
        private readonly List<CycloneConnection> peers = new List<CycloneConnection>();
        private readonly List<CycloneEvent> events = new List<CycloneEvent>();

        private ulong nextPeerId = 1;
        private bool released;

        public CycloneServer(IListenerTransport listener, Schema schema, SessionConfig config)
        {
            this.listener = listener ?? throw new ArgumentNullException(nameof(listener));
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.config = (config ?? new SessionConfig()).Clone();
        }

        public int PeerCount => peers.Count;

        public IListenerTransport Listener => listener;

        public IReadOnlyList<CycloneEvent> Tick(TimeSpan now)
        {
            events.Clear();

            int budget = config.MaxFramesPerTick;
            while (budget > 0)
            {
                var outcome = listener.Accept();
                if (outcome.Status == AcceptStatus.Accepted && outcome.Transport != null)
                {
                    peers.Add(new CycloneConnection(
                        outcome.Transport, schema, config, CycloneRole.Server, now, nextPeerId));
                    nextPeerId++;
                    budget--;
                    continue;
                }
                if (outcome.Status == AcceptStatus.Progress)
                {
                    budget--;
                    continue;
                }
                break;
            }

            for (int index = 0; index < peers.Count; index++)
            {
                var peerEvents = peers[index].Tick(now);
                for (int emitted = 0; emitted < peerEvents.Count; emitted++)
                {
                    events.Add(peerEvents[emitted]);
                }
            }

            for (int index = peers.Count - 1; index >= 0; index--)
            {
                if (peers[index].IsClosed)
                {
                    peers[index].Dispose();
                    peers.RemoveAt(index);
                }
            }

            return events;
        }

        public IReadOnlyList<CycloneEvent> Tick() => Tick(MonotonicClock.Now);

        public SessionState? PeerState(ulong peerId)
        {
            var peer = Find(peerId);
            return peer?.State;
        }

        public bool IsPeerReady(ulong peerId) => Find(peerId)?.IsReady ?? false;

        public SendStatus Send(ulong peerId, uint messageId, ReadOnlySpan<byte> payload)
        {
            var peer = Find(peerId);
            return peer == null ? SendStatus.Closed : peer.Send(messageId, payload);
        }

        public void Broadcast(uint messageId, ReadOnlySpan<byte> payload)
        {
            for (int index = 0; index < peers.Count; index++)
            {
                peers[index].Send(messageId, payload);
            }
        }

        public void Disconnect(ulong peerId) => Find(peerId)?.Close();

        public IReadOnlyList<ulong> PeerIds()
        {
            var ids = new List<ulong>(peers.Count);
            for (int index = 0; index < peers.Count; index++)
            {
                ids.Add(peers[index].PeerId);
            }
            return ids;
        }

        public void Dispose()
        {
            if (released)
            {
                return;
            }
            released = true;
            for (int index = 0; index < peers.Count; index++)
            {
                peers[index].Close();
                peers[index].Dispose();
            }
            peers.Clear();
            listener.Dispose();
        }

        private CycloneConnection? Find(ulong peerId)
        {
            for (int index = 0; index < peers.Count; index++)
            {
                if (peers[index].PeerId == peerId)
                {
                    return peers[index];
                }
            }
            return null;
        }
    }
}
