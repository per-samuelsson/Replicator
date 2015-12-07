using System;
using Starcounter;
using Starcounter.TransactionLog;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Replicator
{
    public interface IWebSocketSender : IDisposable
    {
        Task SendStringAsync(string message, CancellationToken cancellationToken);
        Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);
        Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken);
    }

    [Database]
    public class Replication
    {
        // The GUID of databases we receive from
        public string DatabaseGuid;
        // and the last commit ID we got from them
        public ulong LastCommitID;
    }

    public sealed class Replicator : IDisposable
    {
        private IWebSocketSender _sender;
        private ILogManager _logmanager;
        private CancellationToken _ct;
        private Guid _selfGuid;
        private Guid _peerGuid = Guid.Empty;
        private ILogReader _reader = null;
        private ILogApplicator _applicator = null;
        private byte[] _buf = new byte[1024];

        public Replicator(IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _sender = sender;
            _logmanager = manager;
            _ct = ct;
            _selfGuid = manager.GetDatabaseGuid();
            _sender.SendStringAsync("GUID!" + _selfGuid.ToString(), _ct).ContinueWith(HandleSendResult);
        }

        // Last commit ID received from peer that was successfully committed.
        public ulong LastCommitID
        {
            get
            {
                ulong id = 0;
                if (IsPeerGuidSet)
                {
                    Db.Transact(() =>
                    {
                        Replication repl = Db.SQL<Replication>("SELECT r FROM Replication r WHERE DatabaseGuid = ?", _peerGuid.ToString()).First;
                        if (repl == null)
                        {
                            repl = new Replication()
                            {
                                DatabaseGuid = _peerGuid.ToString(),
                                LastCommitID = 0
                            };
                        }
                        id = repl.LastCommitID;
                    });
                }
                return id;
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

        private Task HandleSentTransaction(Task t, ILogTransaction tran)
        {
            if (t.IsFaulted)
            {
                Console.WriteLine("Replicator: {0}: Transaction {1}: Exception: {2}", _peerGuid, tran.CommitID(), t.Exception.ToString());
                Quit(t.Exception.Message);
                return t;
            }
            if (t.IsCanceled)
            {
                Console.WriteLine("Replicator: {0}: Transaction {1}: Cancelled", _peerGuid, tran.CommitID());
                Quit("Cancelled");
                return t;
            }
            return t;
        }

        private void StartReplication(ulong commitID)
        {
            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");
            _reader = _logmanager.OpenLog(ReplicationParent.TransactionLogDirectory, new LogPosition() { commit_id = commitID }, LogPositionOptions.ReadAfterPosition);
            if (_reader != null)
                HandleOutboundTransaction(null);
        }

        private void HandleOutboundTransaction(Task<ILogTransaction> t)
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

                var tran = t.Result;
                if (tran == null)
                {
                    const string msg = "ILogTransaction was null";
                    Console.WriteLine("Replicator: {0}: {1}", msg);
                    Quit(msg);
                    return;
                }

                if (tran.DatabaseGuid() != _peerGuid)
                {
                    // TODO: user level filtering
                    _sender.SendStringAsync("TRAN!" + JsonConvert.SerializeObject(tran), _ct).ContinueWith((st) => HandleSentTransaction(st, tran));
                }
            }

            _reader.ReadAsync(_ct).ContinueWith(HandleOutboundTransaction);

            return;
        }

        public void HandleStringMessage(string message)
        {
            try
            {
                Console.WriteLine("Replicator {0}: Received from {1}: \"{2}\"", _selfGuid, _peerGuid, message);

                if (message.StartsWith("TRAN!"))
                {
                    if (!IsPeerGuidSet)
                    {
                        Quit("peer GUID not set");
                        return;
                    }

                    ILogTransaction tran = JsonConvert.DeserializeObject<ILogTransaction>(message.Substring(5));

                    Db.Transact(() =>
                    {
                        Replication repl = Db.SQL<Replication>("SELECT r FROM Replication r WHERE DatabaseGuid = ?", _peerGuid.ToString()).First;
                        if (repl == null)
                        {
                            repl = new Replication()
                            {
                                DatabaseGuid = _peerGuid.ToString(),
                                LastCommitID = tran.CommitID()
                            };
                        }
                        else
                        {
                            if (tran.CommitID() <= repl.LastCommitID)
                                throw new Exception("commit ID regression");
                            repl.LastCommitID = tran.CommitID();
                        }
                        _applicator.Apply(tran);
                    });
                    return;
                }

                if (message.StartsWith("QUIT!"))
                {
                    Console.WriteLine("Replicator: \"{0}\"", message);
                    _sender.CloseAsync(1000, null, _ct).ContinueWith((_) =>
                    {
                        Dispose();
                    });
                    return;
                }

                if (message.StartsWith("GUID!"))
                {
                    _peerGuid = Guid.Parse(message.Substring(5).Trim());
                    _sender.SendStringAsync("LAST!" + LastCommitID, _ct).ContinueWith(HandleSendResult);
                    return;
                }

                if (message.StartsWith("LAST!"))
                {
                    StartReplication(UInt64.Parse(message.Substring(5).Trim()));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Replicator: \"{0}\": {1}", message, e);
                Quit(e.Message);
                return;
            }

            Quit("unknown command");
            return;
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
                _peerGuid = Guid.Empty;
            }
            _sender.Dispose();
        }

        public bool IsPeerGuidSet
        {
            get { return _peerGuid != Guid.Empty; }
        }

    }
}
