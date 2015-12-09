using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Starcounter;
using Starcounter.TransactionLog;

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
        // The GUID of the database
        public string DatabaseGuid;
        // and the last LogPosition we got from them
        public ulong Address;
        public ulong Signature;
        public ulong CommitId;
    }

    public sealed class Replicator : IDisposable
    {
        private IWebSocketSender _sender;
        private ILogManager _logmanager;
        private CancellationToken _ct;
        private Guid _selfGuid;
        private Guid _peerGuid;
        private string _peerGuidString;
        private ILogReader _reader = null;
        private ILogApplicator _applicator = null;

        public Replicator(IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _sender = sender;
            _logmanager = manager;
            _ct = ct;
            Input = new ConcurrentQueue<string>();
            Output = new ConcurrentQueue<string>();
            _selfGuid = Program.GetDatabaseGuid();
            PeerGuid = Guid.Empty;
            _sender.SendStringAsync("!GUID " + _selfGuid.ToString(), _ct).ContinueWith(HandleSendResult);
            _applicator = new MockLogApplicator();
        }

        public ConcurrentQueue<string> Input
        {
            get;
            private set;
        }

        public ConcurrentQueue<string> Output
        {
            get;
            private set;
        }

        public Guid PeerGuid
        {
            get
            {
                return _peerGuid;
            }

            set
            {
                _peerGuid = value;
                _peerGuidString = _peerGuid.ToString();
            }
        }

        public string PeerGuidString
        {
            get
            {
                return _peerGuidString;
            }
        }

        public string QuitMessage
        {
            get;
            private set;
        }

        public bool IsClosed
        {
            get
            {
                return IsDisposed || QuitMessage != null;
            }
        }


        public bool IsDisposed
        {
            get;
            private set;
        }

        public bool IsPeerGuidSet
        {
            get { return PeerGuid != Guid.Empty; }
        }

        // Last LogPosition received from peer that was successfully committed.
        public LogPosition LastLogPosition
        {
            get
            {
                LogPosition pos = new LogPosition();
                if (IsPeerGuidSet)
                {
                    Db.Transact(() =>
                    {
                        Replication repl = Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE r.DatabaseGuid = ?", PeerGuidString).First;
                        if (repl != null)
                        {
                            pos.address = repl.Address;
                            pos.signature = repl.Signature;
                            pos.commit_id = repl.CommitId;
                        }
                    });
                }
                return pos;
            }
        }

        private Task HandleSendResult(Task t)
        {
            if (t.IsFaulted)
            {
                Console.WriteLine("Replicator: {0}: Exception: {1}", PeerGuidString, t.Exception.ToString());
                Quit(t.Exception.Message);
                return t;
            }
            if (t.IsCanceled)
            {
                Console.WriteLine("Replicator: {0}: Cancelled", PeerGuidString);
                Quit("Cancelled");
                return t;
            }
            return t;
        }

        private Task HandleSentTransaction(Task t, LogReadResult tran)
        {
            if (t.IsFaulted)
            {
                Console.WriteLine("Replicator: {0}: Transaction {1}: Exception: {2}", PeerGuidString, tran.continuation_position, t.Exception.ToString());
                Quit(t.Exception.Message);
                return t;
            }
            if (t.IsCanceled)
            {
                Console.WriteLine("Replicator: {0}: Transaction {1}: Cancelled", PeerGuidString, tran.continuation_position);
                Quit("Cancelled");
                return t;
            }
            return t;
        }

        private void StartReplication(LogPosition pos)
        {
            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");
            _reader = _logmanager.OpenLog(ReplicationParent.TransactionLogDirectory, pos);
            if (_reader != null)
                HandleOutboundTransaction(null);
        }

        private void HandleOutboundTransaction(Task<LogReadResult> t)
        {
            if (t != null)
            {
                if (t.IsCanceled)
                {
                    Console.WriteLine("Replicator: {0}: Cancelled", PeerGuidString);
                    Quit("Cancelled");
                    return;
                }

                if (t.IsFaulted)
                {
                    Console.WriteLine("Replicator: {0}: {1}", PeerGuidString, t.Exception.ToString());
                    Quit(t.Exception.Message);
                    return;
                }

                var tran = t.Result;
                if (FilterTransaction(t.Result.transaction_data))
                {
                    _sender.SendStringAsync(JsonConvert.SerializeObject(tran), _ct).ContinueWith((st) => HandleSentTransaction(st, tran));
                }
            }

            if (!_ct.IsCancellationRequested)
                _reader.ReadAsync(_ct).ContinueWith(HandleOutboundTransaction);

            return;
        }

        private bool FilterTransaction(TransactionData tran)
        {
            int index;

            index = 0;
            while (index < tran.creates.Count)
            {
                if (tran.creates[index].table.StartsWith("Replicator."))
                {
                    if (tran.creates[index].table == "Replicator.Replication")
                    {
                        for (int i = 0; i < tran.creates[index].columns.Length; i++)
                        {
                            if (tran.creates[index].columns[i].name == "DatabaseGuid")
                            {
                                if ((string)tran.creates[index].columns[i].value == PeerGuidString)
                                    return false;
                            }
                        }
                    }
                    tran.creates.RemoveAt(index);
                }
                else
                {
                    index++;
                }
            }

            index = 0;
            while (index < tran.updates.Count)
            {
                if (tran.updates[index].table.StartsWith("Replicator."))
                {
                    if (tran.creates[index].table == "Replicator.Replication")
                    {
                        for (int i = 0; i < tran.updates[index].columns.Length; i++)
                        {
                            if (tran.updates[index].columns[i].name == "DatabaseGuid")
                            {
                                if ((string)tran.updates[index].columns[i].value == PeerGuidString)
                                    return false;
                            }
                        }
                    }
                    tran.updates.RemoveAt(index);
                }
                else
                {
                    index++;
                }
            }

            index = 0;
            while (index < tran.deletes.Count)
            {
                if (tran.deletes[index].table.StartsWith("Replicator."))
                {
                    tran.deletes.RemoveAt(index);
                }
                else
                {
                    index++;
                }
            }

            if (tran.updates.Count > 0 || tran.creates.Count > 0 || tran.deletes.Count > 0)
                return true;

            // Transaction is empty, don't send it
            return false;
        }

        public void HandleStringMessage(string message)
        {
            try
            {
                Console.WriteLine("Replicator {0}: Received from {1}: \"{2}\"", _selfGuid, PeerGuidString, message);

                if (message[0] != '!')
                {
                    // Transaction processing
                    if (!IsPeerGuidSet)
                    {
                        Quit("peer GUID not set");
                        return;
                    }

                    LogReadResult tran = JsonConvert.DeserializeObject<LogReadResult>(message);
                    Db.Transact(() =>
                    {
                        Replication repl = Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE DatabaseGuid = ?", PeerGuidString).First;
                        if (repl == null)
                        {
                            repl = new Replication()
                            {
                                DatabaseGuid = PeerGuidString,
                                Address = tran.continuation_position.address,
                                Signature = tran.continuation_position.signature,
                                CommitId = tran.continuation_position.commit_id,
                            };
                        }
                        else
                        {
                            // TODO: wait for @bigwad to export comparison for LogPosition
                            // so we can make sure the commit ID is progressing
                            repl.Address = tran.continuation_position.address;
                            repl.Signature = tran.continuation_position.signature;
                            repl.CommitId = tran.continuation_position.commit_id;
                        }
                        _applicator.Apply(tran.transaction_data);
                    });
                    return;
                }

                // Command processing

                if (message.StartsWith("!QUIT"))
                {
                    QuitMessage = message.Substring(5).Trim();
                    _sender.CloseAsync(1000, null, _ct).ContinueWith((_) =>
                    {
                        Dispose();
                    });
                    return;
                }

                if (message.StartsWith("!GUID "))
                {
                    var peerGuid = Guid.Parse(message.Substring(6));
                    if (peerGuid == Guid.Empty)
                    {
                        Quit("empty GUID");
                        return;
                    }
                    if (peerGuid == _selfGuid)
                    {
                        Quit("self GUID");
                        return;
                    }
                    var reply = "!LPOS " + JsonConvert.SerializeObject(LastLogPosition);
                    _sender.SendStringAsync(reply, _ct).ContinueWith(HandleSendResult);
                    return;
                }

                if (message.StartsWith("!LPOS "))
                {
                    StartReplication(JsonConvert.DeserializeObject<LogPosition>(message.Substring(6)));
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Replicator: \"{0}\": {1}", message, e);
                Quit(e.Message);
                return;
            }

            Quit("Unknown command: \"" + message + "\"");
            return;
        }

        public void Quit(string error = "")
        {
            _sender.SendStringAsync("!QUIT " + error, _ct).ContinueWith((t) =>
            {
                _sender.CloseAsync((error == "") ? 1000 : 4000, error, _ct).ContinueWith((_) => 
                {
                    Dispose();
                });
            });
        }

        public void Dispose()
        {
            if (IsDisposed)
                return;
            IsDisposed = true;
            if (IsPeerGuidSet)
            {
                Console.WriteLine("Replicator: {0} disconnected", PeerGuidString);
                PeerGuid = Guid.Empty;
            }
            _sender.Dispose();
        }
    }
}
