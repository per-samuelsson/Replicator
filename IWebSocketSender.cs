using System;
using System.Threading;
using System.Threading.Tasks;

namespace Replicator
{
    public interface IWebSocketSender : IDisposable
    {
        // All of these must be called from a SC thread
        Task SendStringAsync(string message, CancellationToken cancellationToken);
        Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);
        Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken);
    }
}
