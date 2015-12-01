using System;
using System.Collections.Concurrent;
using System.Threading;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;
using System.Threading.Tasks;

namespace Replicator
{
    public class StarcounterWebSocketSender : IWebSocketSender
    {
        private ReplicationSource _source;
        private ulong _wsId;

        public StarcounterWebSocketSender(ReplicationSource source, ulong wsId)
        {
            _source = source;
            _wsId = wsId;
        }

        public Task SendStringAsync(string message, CancellationToken cancellationToken)
        {
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

    public class ReplicationSource
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

        public ReplicationSource(ILogManager manager, CancellationToken ct)
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
                Console.WriteLine("ReplicationSource.HandleStringMessage: {0}", e);
            }
            DisconnectSink("illegal string message", ws);
        }

        private void HandleBinaryMessage(byte[] data, WebSocket ws)
        {
            try
            {
                Replicator sink;
                if (_sinks.TryGetValue(ws.ToUInt64(), out sink))
                {
                    sink.HandleBinaryMessage(new ArraySegment<byte>(data));
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ReplicationSource.HandleBinaryMessage: {0}", e);
            }
            DisconnectSink("illegal binary message", ws);
        }

#if false
        private static void WriteBlob()
        {
            // Returns zero or more transaction blobs.
            //
            // The required argument 'commitId' is a long integer and is the ID
            // of the last transaction that has been processed. The result will
            // return transactions after that commit ID, in order.
            //
            // The 'filter' is expected to be empty (no filtering) or a class name.
            // Optionally, 'filter' may contain a query section with a file hint, 
            // '?N' where N is an positive integer.
            //
            // The response body is binary, consisting of zero or more instances of
            // a four-byte size in network byte order and that many bytes of
            // a single transaction blob data.
            //
            // The query may return with a 204 No Content if no transactions
            // suitable appear in a reasonable time, or it may wait indefinately.
            byte[] body_buf = new byte[256 * 1024];
            int body_len = 0;

            while (body_len + 5 < body_buf.Length)
            {
                if (_rnd.Next(100) < 10)
                    break;
                var pl = new byte[8 + _rnd.Next(1024)];

                pl[0] = (byte)(_nextid >> 56);
                pl[1] = (byte)(_nextid >> 48);
                pl[2] = (byte)(_nextid >> 40);
                pl[3] = (byte)(_nextid >> 32);
                pl[4] = (byte)(_nextid >> 24);
                pl[5] = (byte)(_nextid >> 16);
                pl[6] = (byte)(_nextid >> 8);
                pl[7] = (byte)(_nextid);

                for (int i = 8; i < pl.Length; i++)
                {
                    pl[i] = (byte)_rnd.Next(256);
                }

                if (body_len + 4 + pl.Length > body_buf.Length)
                    break;

                body_buf[body_len + 0] = (byte)(pl.Length >> 24);
                body_buf[body_len + 1] = (byte)(pl.Length >> 16);
                body_buf[body_len + 2] = (byte)(pl.Length >> 8);
                body_buf[body_len + 3] = (byte)(pl.Length);
                body_len += 4;
                pl.CopyTo(body_buf, body_len);
                body_len += pl.Length;
            }
        }
#endif
    }
}
