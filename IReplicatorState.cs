using System;

namespace Replicator
{
    public enum RunState
    {
        Created,
        Starting,
        Running,
        Stopping
    };

    /// <summary>
    /// Allows inspecting the state of a Replicator.
    /// </summary>
    public interface IReplicatorState
    {
        Guid PeerGuid { get; }
        bool IsQuitting { get; }
        RunState RunState { get; }
        ulong TransactionsReceived { get; }
        ulong TransactionsSent { get; }
    }
}
