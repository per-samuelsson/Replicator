using System;
using System.Collections.Generic;
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
            Console.WriteLine("ReplicationChild: Send \"{0}\"", message);
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
            Console.WriteLine("ReplicationChild: Close \"{0}\"", statusMessage);
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

    class ReplicationChild
    {
        const int MinimumReconnectInterval = 1;
        static int MaximumReconnectInterval = 60 * 60;
        private ILogManager _manager;
        private ClientWebSocket _ws = null;
        private Uri _sourceUri;
        private CancellationToken _ct;
        private int _reconnectInterval = MinimumReconnectInterval;
        private Replicator _source = null;

        public ReplicationChild(ILogManager manager, string sourceIp, int sourcePort, CancellationToken ct)
        {
            _manager = manager;
            _sourceUri = new Uri("ws://" + sourceIp + ":" + sourcePort + "/replicator");
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
                    Console.WriteLine("ReplicationChild.Connect: \"{0}\": Cancelled", _sourceUri);
                    return;
                }
                if (t.IsFaulted)
                {
                    Console.WriteLine("ReplicationChild.Connect: \"{0}\": Exception {1}", _sourceUri, t.Exception);
                    return;
                }
            }
            if (_ct.IsCancellationRequested)
                return;
            _ws = new ClientWebSocket();
            _ws.ConnectAsync(_sourceUri, _ct).ContinueWith(HandleConnected);
        }

        public void HandleConnected(Task t)
        {
            if (t.IsCanceled)
            {
                Console.WriteLine("ReplicationChild.HandleConnected: \"{0}\": Cancelled", _sourceUri);
                return;
            }
            if (t.IsFaulted)
            {
                TimeSpan span = TimeSpan.FromMilliseconds(1000 * ReconnectInterval);
                ReconnectInterval = ReconnectInterval * 2;
                Console.WriteLine("ReplicationChild.HandleConnected: \"{0}\": Reconnect in {1}: Exception {2}", _sourceUri, span, t.Exception);
                Task.Delay(span, _ct).ContinueWith(Connect);
                return;
            }
            ReconnectInterval = MinimumReconnectInterval;
            _source = new Replicator(new DotNetWebSocketSender(_ws), _manager, _ct);
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
