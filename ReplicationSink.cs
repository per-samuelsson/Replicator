using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Starcounter.TransactionLog;

namespace Replicator
{
    public class DotNetWebSocketSender : IWebSocketSender
    {
        private WebSocket _ws;

        public DotNetWebSocketSender(WebSocket ws)
        {
            _ws = ws;
        }

        public Task SendStringAsync(string message, CancellationToken cancellationToken)
        {
            return _ws.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(message)), WebSocketMessageType.Text, true, cancellationToken);
        }

        public Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            // make a copy of buffer since this really is async (as opposed to SC websockets)
            var buf = new byte[buffer.Count];
            Array.Copy(buffer.Array, buffer.Offset, buf, 0, buffer.Count);
            return _ws.SendAsync(new ArraySegment<byte>(buf), WebSocketMessageType.Binary, true, cancellationToken);
        }

        public Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken)
        {
            return _ws.CloseAsync((WebSocketCloseStatus)closeStatus, statusMessage, cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _ws.Dispose();
            }
        }
    }

    class ReplicationSink
    {
        const int MinimumReconnectInterval = 1;
        static int MaximumReconnectInterval = 60 * 60;
        private readonly Guid _dbid;
        private ILogManager _manager;
        private ClientWebSocket _ws = new ClientWebSocket();
        private Uri _source;
        private CancellationToken _ct;
        private int _reconnectInterval = MinimumReconnectInterval;
        private ulong _lastCommitId = 0;

        public ReplicationSink(ILogManager manager, string sourceIp, int sourcePort, CancellationToken ct)
        {
            _dbid = new Guid(); // TODO: @bigwad get current database GUID from kernel
            _lastCommitId = 0; // TODO: get last commit ID received from DB
            _manager = manager;
            _source = new Uri("ws://" + sourceIp + ":" + sourcePort + "/replicator/" + _dbid);
            _ct = ct;
            Connect(null);
        }

        public void Cancel()
        {
        }

        public void Connect(Task t)
        {
            if (t != null)
            {
                if (t.IsCanceled)
                {
                    Console.WriteLine("ReplicationSink.Connect: \"{0}\": Cancelled", _source);
                    return;
                }
                if (t.IsFaulted)
                {
                    Console.WriteLine("ReplicationSink.Connect: \"{0}\": Exception {1}", _source, t.Exception);
                    return;
                }
            }
            _ws.ConnectAsync(_source, _ct).ContinueWith(HandleConnected);
        }

        public void HandleConnected(Task t)
        {
            if (t.IsCanceled)
            {
                Console.WriteLine("ReplicationSink.HandleConnected: \"{0}\": Cancelled", _source);
                return;
            }
            if (t.IsFaulted)
            {
                TimeSpan span = TimeSpan.FromMilliseconds(1000 * ReconnectInterval);
                ReconnectInterval = ReconnectInterval * 2;
                Console.WriteLine("ReplicationSink.HandleConnected: \"{0}\": Reconnect in {1}: Exception {2}", _source, span, t.Exception);
                Task.Delay(span, _ct).ContinueWith(Connect);
                return;
            }
            ReconnectInterval = MinimumReconnectInterval;
        }

        // How long to wait between connection attempts, in seconds.
        public int ReconnectInterval
        {
            get
            {
                return _reconnectInterval;
            }
            set
            {
                if (value < MinimumReconnectInterval)
                    value = MinimumReconnectInterval;
                if (value > MaximumReconnectInterval)
                    value = MaximumReconnectInterval;
                _reconnectInterval = value;
            }
        }
    }
}
