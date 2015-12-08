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
        private bool _disposed = false;

        public DotNetWebSocketSender(WebSocket ws)
        {
            _ws = ws;
        }

        public Task SendStringAsync(string message, CancellationToken cancellationToken)
        {
            if (_disposed)
                return Task.FromResult(false);
            Console.WriteLine("ReplicationChild: Send \"{0}\"", message);
            return _ws.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(message)), WebSocketMessageType.Text, true, cancellationToken);
        }

        public Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            if (_disposed)
                return Task.FromResult(false);
            // make a copy of buffer since this really is async (as opposed to SC websockets)
            var buf = new byte[buffer.Count];
            Array.Copy(buffer.Array, buffer.Offset, buf, 0, buffer.Count);
            return _ws.SendAsync(new ArraySegment<byte>(buf), WebSocketMessageType.Binary, true, cancellationToken);
        }

        public Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken)
        {
            if (_disposed)
                return Task.FromResult(false);
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
            if (disposing && !_disposed)
            {
                _disposed = true;
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
        private byte[] _rdbuf = new byte[1024];
        private int _rdlen = 0;
        private Starcounter.DbSession _dbsess = null;

        public ReplicationChild(ILogManager manager, string sourceIp, int sourcePort, CancellationToken ct)
        {
            _manager = manager;
            _sourceUri = new Uri("ws://" + sourceIp + ":" + sourcePort + "/replicator/service");
            _ct = ct;
            _dbsess = new Starcounter.DbSession();
            Connect(null);
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

        public void Reconnect(Exception e = null)
        {
            TimeSpan span = TimeSpan.FromMilliseconds(1000 * ReconnectInterval);
            ReconnectInterval = ReconnectInterval * 2;
            if (e == null)
            {
                Console.WriteLine("ReplicationChild.Reconnect: \"{0}\": Reconnect in {1}", _sourceUri, span);
            }
            else
            {
                Console.WriteLine("ReplicationChild.Reconnect: \"{0}\": Reconnect in {1}: Exception {2}", _sourceUri, span, e);
            }
            Task.Delay(span, _ct).ContinueWith(Connect);
            return;
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
                Reconnect(t.Exception);
            }
            ReconnectInterval = MinimumReconnectInterval;
            _source = new Replicator(new DotNetWebSocketSender(_ws), _manager, _ct);
            _ws.ReceiveAsync(new ArraySegment<byte>(_rdbuf), _ct).ContinueWith(HandleReceive);
        }

        public void HandleReceive(Task<WebSocketReceiveResult> t)
        {
            if (t.IsCanceled)
            {
                Console.WriteLine("ReplicationChild.HandleReceive: \"{0}\": Cancelled", _sourceUri);
                return;
            }
            if (t.IsFaulted)
            {
                Reconnect(t.Exception);
                return;
            }

            WebSocketReceiveResult wsrr = t.Result;
            _rdlen += wsrr.Count;
            if (!wsrr.EndOfMessage)
            {
                if (_rdlen + 1024 > _rdbuf.Length)
                {
                    var newbuf = new byte[_rdbuf.Length * 2];
                    Array.Copy(_rdbuf, newbuf, _rdlen);
                    _rdbuf = newbuf;
                }
                _ws.ReceiveAsync(new ArraySegment<byte>(_rdbuf, _rdlen, _rdbuf.Length - _rdlen), _ct).ContinueWith(HandleReceive);
                return;
            }

            switch (wsrr.MessageType)
            {
                case WebSocketMessageType.Text:
                    var message = Encoding.UTF8.GetString(_rdbuf, 0, _rdlen);
                    _rdlen = 0;
                    _dbsess.RunSync(() =>
                    {
                        _source.HandleStringMessage(message);
                    });
                    break;
                case WebSocketMessageType.Binary:
                    break;
                case WebSocketMessageType.Close:
                    break;
            }

            _rdlen = 0;
            _ws.ReceiveAsync(new ArraySegment<byte>(_rdbuf), _ct).ContinueWith(HandleReceive);
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
