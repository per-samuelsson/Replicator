using System;
using Starcounter;
using Starcounter.TransactionLog;
using System.Threading;
using System.Threading.Tasks;

namespace Replicator
{
    public interface IWebSocketSender : IDisposable
    {
        Task SendStringAsync(string message, CancellationToken cancellationToken);
        Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);
        Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken);
    }

    public sealed class Replicator : IDisposable
    {
        private IWebSocketSender _sender;
        private ILogManager _logmanager;
        private CancellationToken _ct;
        private Guid _selfGuid;
        private Guid _peerGuid;
        private ILogReader _reader = null;
        private ILogApplicator _applicator = null;
        private ulong _tran = 0;
        private byte[] _buf = new byte[1024];

        public Replicator(IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _sender = sender;
            _logmanager = manager;
            _ct = ct;
            _selfGuid = new Guid(); // TODO: get current database GUID from kernel
            _peerGuid = _selfGuid; // mark peer as not identified
            _sender.SendStringAsync("GUID!" + _selfGuid, _ct).ContinueWith(HandleSendResult);
        }

        public ulong LastTransactionId
        {
            get { return _tran; }
            set
            {
                if (value != _tran)
                {
                    _tran = value;
                    _sender.SendStringAsync("TRAN!" + _tran, _ct).ContinueWith(HandleSendResult);
                }
            }
        }

        private Task HandleSendResult(Task t)
        {
            if (t.IsFaulted)
            {
                Console.WriteLine("Replicator: {0}: Exception: {1}", _peerGuid, t.Exception.ToString());
                Quit(t.Exception.Message);
                return t;
            }
            if (t.IsCanceled)
            {
                Console.WriteLine("Replicator: {0}: Cancelled", _peerGuid);
                Quit("Cancelled");
                return t;
            }
            return t;
        }

        private void StartReplication(Task t)
        {
            if (IsPeerGuidSet)
            {
                _reader = _logmanager.OpenLog(ReplicationSource.TransactionLogDirectory, new LogPosition() { commit_id = _tran }, LogPositionOptions.ReadAfterPosition);
                if (_reader != null)
                    HandleOutboundTransaction(null);
            }
            else
            {
                _reader = null;
            }
        }

        private void HandleOutboundTransaction(Task<ReadResult> t)
        {
            if (t != null)
            {
                if (t.IsCanceled)
                {
                    Console.WriteLine("Replicator: {0}: Cancelled", _peerGuid);
                    Quit("Cancelled");
                    return;
                }

                if (t.IsFaulted)
                {
                    Console.WriteLine("Replicator: {0}: {1}", _peerGuid, t.Exception.ToString());
                    Quit(t.Exception.Message);
                    return;
                }

                var result = t.Result;
                if (result == null)
                {
                    const string msg = "ReadResult was null";
                    Console.WriteLine("Replicator: {0}: {1}", msg);
                    Quit(msg);
                    return;
                }

                if (result.bytes_read > _buf.Length - 8)
                {
                    // not enough buffer, realloc and reader will try again
                    _buf = new byte[result.bytes_read + 8];
                }
                else
                {
                    // TODO: filter and stuff
                    // write commit id into buffer prefix
                    _buf[0] = (byte)(result.position.commit_id >> 56);
                    _buf[1] = (byte)(result.position.commit_id >> 48);
                    _buf[2] = (byte)(result.position.commit_id >> 40);
                    _buf[3] = (byte)(result.position.commit_id >> 32);
                    _buf[4] = (byte)(result.position.commit_id >> 24);
                    _buf[5] = (byte)(result.position.commit_id >> 16);
                    _buf[6] = (byte)(result.position.commit_id >> 8);
                    _buf[7] = (byte)(result.position.commit_id);
                    _sender.SendBinaryAsync(new ArraySegment<byte>(_buf, 0, 8 + result.bytes_read), _ct).ContinueWith(HandleSendResult);
                }
            }

            _reader.ReadAsync(_buf, 8, _buf.Length - 8, _ct).ContinueWith(HandleOutboundTransaction);

            return;
        }

        public void HandleStringMessage(string message)
        {
            if (message == "OK!")
                return;

            if (message.StartsWith("QUIT!"))
            {
                Console.WriteLine(message);
                _sender.CloseAsync(1000, null, _ct).ContinueWith((_) =>
                {
                    Dispose();
                });
                return;
            }

            if (message.StartsWith("GUID!"))
            {
                try
                {
                    _peerGuid = Guid.Parse(message.Substring(5).Trim());
                }
                catch (Exception e)
                {
                    Quit(e.Message);
                    return;
                }
            }
            else if (message.StartsWith("TRAN!"))
            {
                try
                {
                    _tran = UInt64.Parse(message.Substring(5).Trim());
                }
                catch (Exception e)
                {
                    Quit(e.Message);
                    return;
                }
            }
            else
            {
                Quit("unknown command");
                return;
            }
            _sender.SendStringAsync("OK!", _ct).Wait();
        }

        public void HandleBinaryMessage(ArraySegment<byte> data)
        {
            if (!IsPeerGuidSet)
            {
                Quit("GUID not set");
                return;
            }
            if (data.Count < 8)
            {
                Quit("binary message too short");
                return;
            }
            ulong commitId = 0;
            for (int i = 0; i < 8; i++)
            {
                commitId <<= 8;
                commitId |= (ulong)data.Array[data.Offset + i];
            }
            // TODO: ensure commit ID sequence correctness
            Db.Transact(() =>
            {
                _applicator.Apply(data.Array, data.Offset + 8, data.Count - 8);
                // TODO: update last commit ID to commitId
            });
        }

        public void Quit(string error = "")
        {
            _sender.SendStringAsync("QUIT!" + error, _ct).ContinueWith((t) =>
            {
                _sender.CloseAsync((error == "") ? 1000 : 4000, error, _ct).ContinueWith((_) => 
                {
                    Dispose();
                });
            });
        }

        public void Dispose()
        {
            if (IsPeerGuidSet)
            {
                Console.WriteLine("Replicator: {0} disconnected", _peerGuid);
                _peerGuid = _selfGuid;
            }
            _sender.Dispose();
        }

        public bool IsPeerGuidSet
        {
            get { return _selfGuid != _peerGuid; }
        }

    }
}
