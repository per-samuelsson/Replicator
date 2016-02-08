using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace LogStreamer
{
    public class StarcounterWebSocketSender : IWebSocketSender
    {
        private LogStreamerParent _source;
        private ulong _wsId;
        private WebSocket _ws;

        public StarcounterWebSocketSender(LogStreamerParent source, ulong wsId)
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

    public class LogStreamerParent
    {
        private readonly ILogManager _logmanager;
        private readonly CancellationToken _ct;
        private readonly Dictionary<string, int> _tablePrios;
        private readonly string _logdirectory = TransactionLogDirectory;
        private ConcurrentDictionary<UInt64, LogStreamerSession> _children = new ConcurrentDictionary<UInt64, LogStreamerSession>();

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

        public LogStreamerParent(ILogManager manager, CancellationToken ct, Dictionary<string, int> tablePrios = null)
        {
            _logmanager = manager;
            _ct = ct;
            _tablePrios = tablePrios;
            Handle.GET(Program.LogStreamerServicePath, (Request req) => HandleConnect(req));
            Handle.WebSocketDisconnect(Program.LogStreamerWebsocketProtocol, HandleDisconnect);
            Handle.WebSocket(Program.LogStreamerWebsocketProtocol, HandleStringMessage);
            Handle.WebSocket(Program.LogStreamerWebsocketProtocol, HandleBinaryMessage);
        }

        private Response HandleConnect(Request req)
        {
            /*
            if (_ct.IsCancellationRequested)
            {
                return new Response()
                {
                    StatusCode = 503,
                    StatusDescription = "Service Unavailable"
                };
            }
            */

            try
            {
                if (!req.WebSocketUpgrade)
                {
                    return new Response()
                    {
                        StatusCode = 400,
                        StatusDescription = "Bad Request"
                    };
                }
                UInt64 wsId = req.GetWebSocketId();
                WebSocket ws = req.SendUpgrade(Program.LogStreamerWebsocketProtocol, null, null, null);
                _children[wsId] = new LogStreamerSession(new StarcounterWebSocketSender(this, wsId), _logmanager, _ct, _tablePrios);
                return HandlerStatus.Handled;
            }
            catch (Exception exc)
            {
                return new Response()
                {
                    StatusCode = 500,
                    StatusDescription = "Internal Server Error",
                    Body = exc.ToString()
                };
            }
        }

        private void HandleDisconnect(WebSocket ws)
        {
            LogStreamerSession sink;
            if (_children.TryRemove(ws.ToUInt64(), out sink))
                sink.Dispose();
        }

        private void DisconnectSink(string error, WebSocket ws)
        {
            LogStreamerSession sink;
            if (_children.TryGetValue(ws.ToUInt64(), out sink))
            {
                sink.Quit(error);
                return;
            }
            ws.Send("!QUIT UnknownSocket");
            ws.Disconnect("Unknown socket");
        }

        public void SinkDisposed(ulong wsId)
        {
            LogStreamerSession sink;
            _children.TryRemove(wsId, out sink);
        }

        private void HandleStringMessage(string data, WebSocket ws)
        {
            try
            {
                LogStreamerSession child;
                if (_children.TryGetValue(ws.ToUInt64(), out child))
                {
                    child.Input.Enqueue(data);
                    child.ProcessInput();
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("LogStreamerParent.HandleStringMessage: {0}", e);
            }
            DisconnectSink("illegal string message", ws);
        }

        private void HandleBinaryMessage(byte[] data, WebSocket ws)
        {
            DisconnectSink("illegal binary message", ws);
        }
    }
}
