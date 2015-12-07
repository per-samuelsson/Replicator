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

        public StarcounterWebSocketSender(ReplicationParent source, ulong wsId)
        {
            _source = source;
            _wsId = wsId;
        }

        public Task SendStringAsync(string message, CancellationToken cancellationToken)
        {
            Console.WriteLine("ReplicationParent: Send \"{0}\"", message);
            (new WebSocket(_wsId)).Send(message);
            return Task.FromResult(false);
        }

        public Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            byte[] buf = buffer.Array;
            if (buffer.Offset != 0)
            {
                buf = new byte[buffer.Count];
                Array.Copy(buffer.Array, buffer.Offset, buf, 0, buffer.Count);
            }
            (new WebSocket(_wsId)).Send(buf, buffer.Count);
            return Task.FromResult(false);
        }

        public Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken)
        {
            Console.WriteLine("ReplicationParent: Close \"{0}\"", statusMessage);
            (new WebSocket(_wsId)).Disconnect(statusMessage, (Starcounter.WebSocket.WebSocketCloseCodes)closeStatus);
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
                if (_wsId != 0)
                {
                    _source.SinkDisposed(_wsId);
                    _wsId = 0;
                }
            }
        }
    }

    public class ReplicationParent
    {
        const string ProtocolString = "sc-replicator";
        private ConcurrentDictionary<UInt64, Replicator> _sinks = new ConcurrentDictionary<UInt64, Replicator>();
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
            Handle.GET("/replicator", (Request req) => HandleConnect(req));
            Handle.WebSocketDisconnect(ProtocolString, HandleDisconnect);
            Handle.WebSocket(ProtocolString, HandleStringMessage);
            Handle.WebSocket(ProtocolString, HandleBinaryMessage);
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
                _sinks[wsId] = new Replicator(new StarcounterWebSocketSender(this, wsId), _logmanager, _ct);
                Dictionary<String, String> headers = new Dictionary<String, String>() {
                            { "WebSocketId", wsId.ToString() }
                        };
                Session s = new Session();
                WebSocket ws = req.SendUpgrade(ProtocolString, null, headers, s);
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
            if (_sinks.TryRemove(ws.ToUInt64(), out sink))
                sink.Dispose();
        }

        private void DisconnectSink(string error, WebSocket ws)
        {
            Replicator sink;
            if (_sinks.TryGetValue(ws.ToUInt64(), out sink))
            {
                sink.Quit(error);
                return;
            }
        }

        public void SinkDisposed(ulong wsId)
        {
            Replicator sink;
            if (!_sinks.TryRemove(wsId, out sink))
            {
                Console.WriteLine("sink wsID={0} not found", wsId);
                throw new Exception("sink not found");
            }
            Console.WriteLine("sink wsID={0} disposed", wsId);
        }

        private void HandleStringMessage(string data, WebSocket ws)
        {
            try
            {
                Replicator sink;
                if (_sinks.TryGetValue(ws.ToUInt64(), out sink))
                {
                    sink.HandleStringMessage(data);
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
