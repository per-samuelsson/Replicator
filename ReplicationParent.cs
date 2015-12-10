using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace Replicator
{
    public class StarcounterWebSocketSender : IWebSocketSender
    {
        private ReplicationParent _source;
        private ulong _wsId;
        private WebSocket _ws;

        public StarcounterWebSocketSender(ReplicationParent source, ulong wsId)
        {
            _source = source;
            _wsId = wsId;
            _ws = new WebSocket(wsId);
        }

        public WebSocket Socket
        {
            get { return _ws; }
        }

        // Must run on a SC thread
        public Task SendStringAsync(string message, CancellationToken cancellationToken)
        {
            Socket.Send(message);
            return Task.FromResult(false);
        }

        // Must run on a SC thread
        public Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            byte[] buf = buffer.Array;
            if (buffer.Offset != 0)
            {
                buf = new byte[buffer.Count];
                Array.Copy(buffer.Array, buffer.Offset, buf, 0, buffer.Count);
            }
            Socket.Send(buf, buffer.Count);
            return Task.FromResult(false);
        }

        // Must run on a SC thread
        public Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken)
        {
            Socket.Disconnect(statusMessage, (Starcounter.WebSocket.WebSocketCloseCodes)closeStatus);
            return Task.FromResult(false);
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
                var wsId = _wsId;
                _wsId = 0;
                if (wsId != 0)
                {
                    _source.SinkDisposed(wsId);
                }
            }
        }
    }

    public class ReplicationParent
    {
        private DbSession _dbsess;
        private ConcurrentDictionary<UInt64, Replicator> _children = new ConcurrentDictionary<UInt64, Replicator>();
        private string _logdirectory = TransactionLogDirectory;
        private ILogManager _logmanager;
        private CancellationToken _ct;

        static public string TransactionLogDirectory
        {
            get
            {
                string settingsUri = "http://127.0.0.1:" + StarcounterEnvironment.Default.SystemHttpPort + "/api/admin/databases/" + StarcounterEnvironment.DatabaseNameLower + "/settings";
                var settingsResult = Http.GET(settingsUri);
                if (settingsResult == null || !settingsResult.IsSuccessStatusCode)
                    throw new Exception("Can't access " + settingsUri);
                dynamic json = new Json(settingsResult.Body);
                return json.TransactionLogDirectory;
            }
        }

        public ReplicationParent(ILogManager manager, CancellationToken ct)
        {
            _logmanager = manager;
            _ct = ct;
            _dbsess = new DbSession();
            Handle.GET(Program.ReplicatorServicePath, (Request req) => HandleConnect(req));
            Handle.WebSocketDisconnect(Program.ReplicatorWebsocketProtocol, HandleDisconnect);
            Handle.WebSocket(Program.ReplicatorWebsocketProtocol, HandleStringMessage);
            Handle.WebSocket(Program.ReplicatorWebsocketProtocol, HandleBinaryMessage);
        }

        private Response HandleConnect(Request req)
        {
            try
            {
                if (!req.WebSocketUpgrade)
                {
                    return new Response()
                    {
                        StatusCode = 400
                    };
                }
                UInt64 wsId = req.GetWebSocketId();
                WebSocket ws = req.SendUpgrade(Program.ReplicatorWebsocketProtocol, null, null, null);
                _children[wsId] = new Replicator(true, _dbsess, new StarcounterWebSocketSender(this, wsId), _logmanager, _ct);
                return HandlerStatus.Handled;
            }
            catch (Exception exc)
            {
                return new Response()
                {
                    StatusCode = 500,
                    Body = exc.ToString()
                };
            }
        }

        private void HandleDisconnect(WebSocket ws)
        {
            Replicator sink;
            if (_children.TryRemove(ws.ToUInt64(), out sink))
                sink.Dispose();
        }

        private void DisconnectSink(string error, WebSocket ws)
        {
            Replicator sink;
            if (_children.TryGetValue(ws.ToUInt64(), out sink))
            {
                sink.Quit(error);
                return;
            }
        }

        public void SinkDisposed(ulong wsId)
        {
            Replicator sink;
            if (!_children.TryRemove(wsId, out sink))
            {
                Console.WriteLine("sink wsID={0} not found", wsId);
                return;
                // throw new Exception("sink not found");
            }
            Console.WriteLine("sink wsID={0} disposed", wsId);
        }

        private void HandleStringMessage(string data, WebSocket ws)
        {
            try
            {
                Replicator child;
                if (_children.TryGetValue(ws.ToUInt64(), out child))
                {
                    child.Input.Enqueue(data);
                    child.ProcessInput();
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ReplicationParent.HandleStringMessage: {0}", e);
            }
            DisconnectSink("illegal string message", ws);
        }

        private void HandleBinaryMessage(byte[] data, WebSocket ws)
        {
            DisconnectSink("illegal binary message", ws);
        }
    }
}
