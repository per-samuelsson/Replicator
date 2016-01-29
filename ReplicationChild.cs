using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Starcounter.Internal;
using Starcounter.TransactionLog;
using System.Runtime.ExceptionServices;

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
            if (!_disposed)
            {
                // if (_ws.State == WebSocketState.Open)
                {
                    return _ws.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(message)), WebSocketMessageType.Text, true, cancellationToken);
                }
            }
            return Task.FromResult(false);
        }

        public Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            if (!_disposed)
            {
                if (_ws.State == WebSocketState.Open)
                {
                    // make a copy of buffer since this really is async (as opposed to SC websockets)
                    var buf = new byte[buffer.Count];
                    Array.Copy(buffer.Array, buffer.Offset, buf, 0, buffer.Count);
                    return _ws.SendAsync(new ArraySegment<byte>(buf), WebSocketMessageType.Binary, true, cancellationToken);
                }
            }
            return Task.FromResult(false);
        }

        public Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken)
        {
            if (!_disposed)
            {
                // if (_ws.State == WebSocketState.Open)
                {
                    return _ws.CloseOutputAsync((WebSocketCloseStatus)closeStatus, statusMessage, cancellationToken);
                }
            }
            return Task.FromResult(false);
        }

        public bool IsDisposed
        {
            get { return _disposed; }
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
        private readonly ILogManager _manager;
        private readonly Uri _sourceUri;
        private readonly CancellationToken _ct;
        private readonly Dictionary<string, int> _tablePrios;
        private ClientWebSocket _ws = null;
        private int _reconnectInterval = Program.ReconnectMinimumWaitSeconds;
        private Replicator _source = null;
        private byte[] _rdbuf = new byte[1024];
        private int _rdlen = 0;
        private Starcounter.DbSession _dbsess = null;
        private int _reconnectMinimum;
        private int _reconnectMaximum;
        private bool _isConnected = false;

        public ReplicationChild(ILogManager manager, string parentUri, CancellationToken ct, Dictionary<string, int> tablePrios = null)
        {
            _manager = manager;
            if (parentUri == "")
                parentUri = "ws://" + System.Environment.MachineName + ":" + StarcounterEnvironment.Default.UserHttpPort + Program.ReplicatorServicePath;
            if (parentUri.IndexOf("//") < 0)
                parentUri = "ws://" + parentUri;
            if (parentUri.IndexOf('/', parentUri.IndexOf("//") + 2) < 0)
                parentUri = parentUri + Program.ReplicatorServicePath;
            _sourceUri = new Uri(parentUri);
            _ct = ct;
            _tablePrios = tablePrios;
            _dbsess = new Starcounter.DbSession();
            _reconnectMinimum = Program.ReconnectMinimumWaitSeconds;
            _reconnectMaximum = Program.ReconnectMaximumWaitSeconds;
            Connect(null);
        }

        public void Connect(Task t)
        {
            IsConnected = false;
            if (t != null)
            {
                if (t.IsCanceled)
                {
                    return;
                }
                if (t.IsFaulted)
                {
                    ExceptionDispatchInfo.Capture(t.Exception).Throw();
                    return;
                }
            }
            if (_ct.IsCancellationRequested)
            {
                return;
            }
            Program.Status = "Connecting to " + _sourceUri.ToString();
            _ws = new ClientWebSocket();
            _ws.Options.KeepAliveInterval = TimeSpan.FromDays(1);
            _ws.ConnectAsync(_sourceUri, _ct).ContinueWith(HandleConnected);
        }

        public void Reconnect(Exception e = null)
        {
            IsConnected = false;
            string msg = null;
            if (_source != null)
            {
                // TODO: need better heuristic on this
                if (_source.TransactionsReceived > 0 || _source.TransactionsSent > 0)
                {
                    ReconnectInterval = _reconnectMinimum;
                }
                msg = _source.QuitMessage;
                _source.Dispose();
                _source = null;
            }
            if (e != null)
            {
                if (msg == null || msg == "")
                {
                    if (e.InnerException == null)
                        msg = e.Message;
                    else
                        msg = e.InnerException.Message;
                }
                Console.WriteLine("ReplicationChild.Reconnect: \"{0}\": Exception {1}", _sourceUri, e);
            }
            if (msg == null)
                msg = "";
            if (msg != "")
                msg = "\"" + msg + "\": ";
            if (_ct.IsCancellationRequested)
            {
                Program.Status = _sourceUri.ToString() + ": " + msg + "Cancelled";
                return;
            }
            TimeSpan span = TimeSpan.FromMilliseconds(1000 * ReconnectInterval);
            Program.Status = _sourceUri.ToString() + ": " + msg + "Reconnect at " + (DateTime.Now + span);
            ReconnectInterval = ReconnectInterval * 2;
            Task.Delay(span, _ct).ContinueWith(Connect);
            return;
        }

        public void HandleConnected(Task t)
        {
            if (t.IsCanceled || _ct.IsCancellationRequested)
            {
                return;
            }
            if (t.IsFaulted)
            {
                Reconnect(t.Exception);
                return;
            }
            Program.Status = "Connected to " + _sourceUri.ToString();
            _source = new Replicator(_dbsess, new DotNetWebSocketSender(_ws), _manager, _ct, _tablePrios);
            _ws.ReceiveAsync(new ArraySegment<byte>(_rdbuf), _ct).ContinueWith(HandleReceive);
        }

        private void HandleDisconnected(Task t)
        {
            if (t.IsFaulted)
            {
                Reconnect(t.Exception);
                return;
            }
            Reconnect(null);
        }

        public bool IsConnected
        {
            get
            {
                return _isConnected;
            }
            private set
            {
                if (_isConnected != value)
                {
                    if (value)
                    {
                        Program.ParentStatus.DatabaseGuid = _source.PeerGuidString;
                        _isConnected = true;
                    }
                    else
                    {
                        Program.ParentStatus.DatabaseGuid = "";
                        _isConnected = false;
                    }
                }
            }
        }

        public void HandleReceive(Task<WebSocketReceiveResult> t)
        {
            if (t.IsCanceled)
            {
                if (_source != null)
                {
                    _dbsess.RunAsync(_source.Canceled);
                }
                return;
            }
            if (t.IsFaulted)
            {
                Reconnect(t.Exception);
                return;
            }
            if (!IsConnected && _source.IsPeerGuidSet)
            {
                IsConnected = true;
            }

            WebSocketReceiveResult wsrr = t.Result;
            _rdlen += wsrr.Count;

            if (wsrr.EndOfMessage)
            {
                switch (wsrr.MessageType)
                {
                    case WebSocketMessageType.Text:
                        var message = Encoding.UTF8.GetString(_rdbuf, 0, _rdlen);
                        _source.Input.Enqueue(message);
                        _source.HandleInput();
                        break;
                    case WebSocketMessageType.Binary:
                        break;
                    case WebSocketMessageType.Close:
                        _ws.CloseOutputAsync(wsrr.CloseStatus ?? WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None).ContinueWith(HandleDisconnected);
                        return;
                }
                _rdlen = 0;
            }

            try
            {
                if (_rdlen + 1024 > _rdbuf.Length)
                {
                    var newbuf = new byte[_rdbuf.Length * 2];
                    Array.Copy(_rdbuf, newbuf, _rdlen);
                    _rdbuf = newbuf;
                }
                _ws.ReceiveAsync(new ArraySegment<byte>(_rdbuf, _rdlen, _rdbuf.Length - _rdlen), _ct).ContinueWith(HandleReceive);
            }
            catch (Exception e)
            {
                Reconnect(e);
            }
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
                if (value < _reconnectMinimum)
                    value = _reconnectMinimum;
                if (value > _reconnectMaximum)
                    value = _reconnectMaximum;
                _reconnectInterval = value;
            }
        }
    }
}
