using System;

namespace LogStreamer
{
    public enum RunState
    {
        Created,
        Starting,
        Running,
        Stopping
    };

    /// <summary>
    /// Allows inspecting the state of a LogStreamer.
    /// </summary>
    public interface ILogStreamerState
    {
        Guid PeerGuid { get; }
        bool IsQuitting { get; }
        RunState RunState { get; }
        ulong TransactionsReceived { get; }
        ulong TransactionsSent { get; }
    }
}
