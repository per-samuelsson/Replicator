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
        // All of these ,ust be called from a SC thread
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
        private DbSession _dbsess;
        private IWebSocketSender _sender;
        private ILogManager _logmanager;
        private CancellationToken _ct;
        private Guid _selfGuid;
        private Guid _peerGuid;
        private string _peerGuidString;
        private ILogReader _reader = null;
        private ILogApplicator _applicator = null;
        private SemaphoreSlim _inputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _input = new ConcurrentQueue<string>();
        private SemaphoreSlim _logQueueSem = new SemaphoreSlim(1);
        private ConcurrentQueue<Task<LogReadResult>> _logQueue = new ConcurrentQueue<Task<LogReadResult>>();

        public Replicator(DbSession dbsess, IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _dbsess = dbsess;
            _sender = sender;
            _logmanager = manager;
            _ct = ct;
            _selfGuid = Program.GetDatabaseGuid();
            PeerGuid = Guid.Empty;
            _sender.SendStringAsync("!GUID " + _selfGuid.ToString(), _ct).ContinueWith(HandleSendResult);
            _applicator = new MockLogApplicator();
        }

        // Must run in a SC thread
        public void ProcessInput()
        {
            if (_inputSem.Wait(0))
            {
                try
                {
                    string message;
                    while (Input.TryDequeue(out message))
                        ProcessStringMessage(message);
                }
                finally
                {
                    _inputSem.Release();
                }
            }
        }

        // May run in any thread
        public void HandleInput()
        {
            if (_inputSem.Wait(0))
            {
                _inputSem.Release();
                _dbsess.RunAsync(ProcessInput);
            }
            return;
        }

        // Websocket input string queue
        public ConcurrentQueue<string> Input
        {
            get { return _input; }
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
        // Must run on a SC thread
        private LogPosition LastLogPosition
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

        // Must run on a SC thread
        private void ProcessFailedSendResult(Task t)
        {
            if (t.IsFaulted)
            {
                Console.WriteLine("Replicator: {0}: Exception: {1}", PeerGuidString, t.Exception.ToString());
                Quit(t.Exception.Message);
                return;
            }
            if (t.IsCanceled)
            {
                Console.WriteLine("Replicator: {0}: Cancelled", PeerGuidString);
                Quit("Cancelled");
                return;
            }
        }

        // Can be called from non-SC thread
        private void HandleSendResult(Task t)
        {
            if (t.IsFaulted || t.IsCanceled)
                _dbsess.RunAsync(() => { ProcessFailedSendResult(t); });
            return;
        }

        private void StartReplication(LogPosition pos)
        {
            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");
            _reader = _logmanager.OpenLog(ReplicationParent.TransactionLogDirectory, pos);
            if (_reader != null)
                _reader.ReadAsync(_ct).ContinueWith(HandleOutboundTransaction);
        }

        // Called from non-SC thread (LogReader)
        private void HandleOutboundTransaction(Task<LogReadResult> t)
        {
            _logQueue.Enqueue(t);
            if (_logQueueSem.Wait(0, _ct))
            {
                _logQueueSem.Release();
                _dbsess.RunAsync(ProcessLogQueue);
            }
            return;
        }

        // Must run on a SC thread
        private void ProcessLogQueue()
        {
            if (_logQueueSem.Wait(0))
            {
                try
                {
                    Task<LogReadResult> t;
                    while (_logQueue.TryDequeue(out t))
                        ProcessOutboundTransaction(t);
                }
                finally
                {
                    if (!_ct.IsCancellationRequested)
                        _reader.ReadAsync(_ct).ContinueWith(HandleOutboundTransaction);
                    _logQueueSem.Release();
                }
            }
        }

        // Must run on a SC thread
        private void ProcessOutboundTransaction(Task<LogReadResult> t)
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

            LogReadResult lrr = t.Result;
            if (FilterTransaction(lrr.transaction_data))
            {
                _sender.SendStringAsync(JsonConvert.SerializeObject(lrr), _ct).ContinueWith(HandleSendResult);
            }
        }

        // Must run on a SC thread
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

        // Must run on a SC thread
        private void ProcessStringMessage(string message)
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
                            repl.DatabaseGuid = PeerGuidString; // needed so it shows up in the outbound log
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
                        Quit("GUID is empty");
                        return;
                    }
                    if (peerGuid == _selfGuid)
                    {
                        Quit("GUID is my own");
                        return;
                    }
                    if (peerGuid.ToString() == Program.ParentGuid)
                    {
                        Quit("GUID is parents");
                        return;
                    }
                    PeerGuid = peerGuid;
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

        // Must run on a SC thread
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
            if (!IsDisposed)
            {
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
}
