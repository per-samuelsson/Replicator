using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Starcounter;
using Starcounter.TransactionLog;
using System.IO;
using System.Text;

namespace Replicator
{
    public interface IWebSocketSender : IDisposable
    {
        // All of these must be called from a SC thread
        Task SendStringAsync(string message, CancellationToken cancellationToken);
        Task SendBinaryAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);
        Task CloseAsync(int closeStatus, string statusMessage, CancellationToken cancellationToken);
    }

    [Database]
    public class Replication
    {
        // The GUID of the database + ':' + table name
        public string TableId;
        // and the last LogPosition we got from them for that table
        public ulong CommitId;
    }

    public sealed class Replicator : IDisposable
    {
        private bool _isServer;
        private bool _isConnected;
        private DbSession _dbsess;
        private IWebSocketSender _sender;
        private Guid _selfGuid;
        private Guid _peerGuid;
        private LogApplicator _applicator = null;
        private bool _isQuitting = false;

        private ConcurrentQueue<KeyValuePair<string, ulong>> _replicationStateQueue = null;
        private Dictionary<string, ulong> _peerTablePositions = null;
        private readonly HashSet<string> _tableFilter = null;

        // used to synchronize websocket input to the replicator
        private SemaphoreSlim _inputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _input = new ConcurrentQueue<string>();

        private FilteredLogReader _reader = null;
        private ConcurrentQueue<Task<LogReadResult>> _logQueue = new ConcurrentQueue<Task<LogReadResult>>();
        private SemaphoreSlim _logQueueSem = new SemaphoreSlim(1);

        public Replicator(bool isServer, DbSession dbsess, IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _isServer = isServer;
            _isConnected = false;
            _dbsess = dbsess;
            _sender = sender;
            LogManager = manager;
            CancellationToken = ct;
            _selfGuid = Program.GetDatabaseGuid();
            PeerGuid = Guid.Empty;
            DefaultPriority = 1; // default prio (0 = don't replicate, 1 = lowest prio)
            _sender.SendStringAsync("!GUID " + _selfGuid.ToString(), CancellationToken).ContinueWith(HandleSendResult);
            _applicator = new LogApplicator();
        }

        public ILogManager LogManager
        {
            get;
            private set;
        }

        public CancellationToken CancellationToken
        {
            get;
            private set;
        }

        public ulong DefaultPriority
        {
            get;
            set;
        }

        public bool IsConnected
        {
            get
            {
                return _isConnected;
            }
            set
            {
                if (value != _isConnected)
                {
                    if (value)
                    {
                        _isConnected = true;
                    }
                    else
                    {
                        _isConnected = false;
                    }
                }
            }
        }

        public string StripDatabasePrefix(string tableNameOrId)
        {
            if (tableNameOrId.IndexOf(':') == -1)
            {
                return tableNameOrId;
            }
            if (tableNameOrId.StartsWith(PeerTableIdPrefix))
            {
                return tableNameOrId.Substring(PeerTableIdPrefix.Length);
            }
            throw new ArgumentException("tableNameOrId had wrong database id prefix");
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
                PeerGuidString = _peerGuid.ToString();
                PeerTableIdPrefix = PeerGuidString + ':';
            }
        }

        public string PeerGuidString
        {
            get;
            private set;
        }

        public string PeerTableIdPrefix
        {
            get;
            private set;
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
                LogPosition pos = new LogPosition()
                {
                    commit_id = 0
                };
                if (IsPeerGuidSet)
                {
                    Db.Transact(() =>
                    {
                        Replication repl = Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE r.DatabaseGuid = ?", PeerGuidString).First;
                        if (repl != null)
                        {
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

        // Must run on a SC thread
        private void StartReplication()
        {
            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");
            _reader = new FilteredLogReader(this, _peerTablePositions, _tableFilter);
            _reader.ReadAsync(CancellationToken).ContinueWith(HandleOutboundTransaction);
        }

        // Called from non-SC thread (LogReader)
        private void HandleOutboundTransaction(Task<LogReadResult> t)
        {
            _logQueue.Enqueue(t);
            if (_logQueueSem.Wait(0))
            {
                _logQueueSem.Release();
                _dbsess.RunAsync(ProcessLogQueue);
            }
            return;
        }

        public void RunProcessLogQueue()
        {
            if (_logQueueSem.Wait(0))
            {
                _logQueueSem.Release();
                _dbsess.RunAsync(ProcessLogQueue);
            }
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
                catch (Exception e)
                {
                    Console.WriteLine("Replicator: {0}: ProcessLogQueue: {1}", PeerGuidString, e);
                }
                finally
                {
                    if (!CancellationToken.IsCancellationRequested)
                        _reader.ReadAsync(CancellationToken).ContinueWith(HandleOutboundTransaction);
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

            _sender.SendStringAsync(StringSerializer.Serialize(new StringBuilder(), t.Result).ToString(), CancellationToken).ContinueWith(HandleSendResult);
        }

        /*
        private bool FilterLoops(string table, column_update[] columns)
        {
            if (table == "Replicator.Replication")
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    if (columns[i].name == "DatabaseGuid")
                    {
                        if ((string)columns[i].value == PeerGuidString)
                            return true;
                    }
                }
            }
            return false;
        }

        // Must run on a SC thread
        private bool FilterTransaction(LogReadResult lrr)
        {
            try
            {
                var tran = lrr.transaction_data;
                int index;

                lock (_prioMap)
                {
                    index = 0;
                    while (index < tran.creates.Count)
                    {
                        var record = tran.creates[index];
                        if (record.table.StartsWith("Replicator."))
                        {
                            if (FilterLoops(record.table, record.columns))
                                return false;
                            tran.creates.RemoveAt(index);
                        }
                        else
                        {
                            ulong p = DefaultPriority;
                            if (_filter != null)
                            {
                                p = _filter.FilterCreate(PeerGuidString, record);
                            }
                            if (p == 0)
                            {
                                tran.creates.RemoveAt(index);
                            }
                            else
                            {
                                tran.creates[index] = record;
                                index++;
                            }
                        }
                    }

                    index = 0;
                    while (index < tran.updates.Count)
                    {
                        var record = tran.updates[index];
                        if (record.table.StartsWith("Replicator."))
                        {
                            if (FilterLoops(record.table, record.columns))
                                return false;
                            tran.creates.RemoveAt(index);
                        }
                        else
                        {
                            ulong p = DefaultPriority;
                            if (_filter != null)
                            {
                                p = _filter.FilterUpdate(PeerGuidString, record);
                            }
                            if (p == 0)
                            {
                                tran.creates.RemoveAt(index);
                            }
                            else
                            {
                                tran.updates[index] = record;
                                index++;
                            }
                        }
                    }

                    index = 0;
                    while (index < tran.deletes.Count)
                    {
                        var record = tran.deletes[index];
                        if (record.table.StartsWith("Replicator."))
                        {
                            tran.deletes.RemoveAt(index);
                        }
                        else
                        {
                            ulong p = DefaultPriority;
                            if (_filter != null)
                            {
                                p = _filter.FilterDelete(PeerGuidString, record);
                            }
                            if (p == 0)
                            {
                                tran.deletes.RemoveAt(index);
                            }
                            else
                            {
                                tran.deletes[index] = record;
                                index++;
                            }
                        }
                    }
                }

                if (tran.updates.Count > 0 || tran.creates.Count > 0 || tran.deletes.Count > 0)
                    return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Replicator: {0}: FilterTransaction {1}: {2}", PeerGuidString, lrr.continuation_position, e);
            }

            // Transaction is empty or exception occured, don't send it
            return false;
        }
        */

        // Must run on a SC thread
        private void UpdateCommitId(string table, ulong commitId)
        {
            string tableId = PeerTableIdPrefix + table;
            Replication repl = Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE TableId = ?", tableId).First;
            if (repl == null)
            {
                repl = new Replication()
                {
                    TableId = tableId,
                    CommitId = commitId,
                };
            }
            else
            {
                repl.TableId = tableId; // needed so it shows up in the outbound log
                repl.CommitId = commitId;
            }
        }

        private void ForAllTablesInTransaction(LogReadResult tran, Action<string> action)
        {
            for (int index = tran.transaction_data.creates.Count - 1; index >= 0; index--)
                action(tran.transaction_data.creates[index].table);
            for (int index = tran.transaction_data.updates.Count - 1; index >= 0; index--)
                action(tran.transaction_data.updates[index].table);
            for (int index = tran.transaction_data.deletes.Count - 1; index >= 0; index--)
                action(tran.transaction_data.deletes[index].table);
            return;
        }

        // Must run on a SC thread
        private void ProcessIncomingTransaction(LogReadResult tran)
        {
            try
            {
                Db.Transact(() =>
                {
                    ulong commitId = tran.continuation_position.commit_id;
                    if (_tableFilter == null)
                    {
                        UpdateCommitId("", commitId);
                    }
                    else
                    {
                        ForAllTablesInTransaction(tran, (tableName) => UpdateCommitId(tableName, commitId));
                    }
                    _applicator.Apply(tran.transaction_data);
                });
            }
            catch (Exception e)
            {
                uint code;
                // Special handling for ScErrTableNotFound (SCERR4230) and ScErrSchemaCodeMismatch (SCERR4177)
                if (ErrorCode.TryGetCode(e, out code))
                {
                    if (code == 4230 || code == 4177)
                    {
                        var tableset = new HashSet<string>();
                        ForAllTablesInTransaction(tran, (tableName) => tableset.Add(tableName));
                        Db.Transact(() =>
                        {
                            foreach (Starcounter.Metadata.ClrClass cc in Db.SQL<Starcounter.Metadata.ClrClass>("SELECT cc FROM Starcounter.Metadata.ClrClass cc"))
                            {
                                tableset.Remove(cc.FullClassName);
                            }
                        });
                        if (tableset.Count > 0)
                        {
                            var sb = new StringBuilder();
                            sb.Append("Class not loaded:");
                            foreach (var tableName in tableset)
                            {
                                sb.Append(' ');
                                sb.Append(tableName);
                            }
                            Quit(sb.ToString());
                            return;
                        }
                    }
                }
                throw e;
            }
        }

        // Must run on a SC thread
        private void ProcessStringMessage(string message)
        {
            if (_isQuitting)
            {
                return;
            }

            try
            {
                if (message[0] != '!')
                {
                    // Transaction processing
                    if (!IsPeerGuidSet)
                    {
                        Quit("peer GUID not set");
                        return;
                    }
                    ProcessIncomingTransaction(StringSerializer.DeserializeLogReadResult(new StringReader(message)));
                    return;
                }


                // Command processing

                if (message.StartsWith("!QUIT"))
                {
                    QuitMessage = message.Substring(5).Trim();
                    _sender.CloseAsync(1000, null, CancellationToken).ContinueWith((_) =>
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
                    PeerGuid = peerGuid;
                    BuildReplicationState();
                    SendReplicationState();
                    return;
                }

                if (message.StartsWith("!TPOS "))
                {
                    if (_peerTablePositions == null)
                        _peerTablePositions = new Dictionary<string, ulong>();
                    var sr = new StringReader(message);
                    for (int i = 0; i < 6; i++) sr.Read(); // skip first 6 chars
                    while (StringSerializer.SkipWS(sr) != -1)
                    {
                        _peerTablePositions.Add(StringSerializer.DeserializeString(sr), StringSerializer.DeserializeULong(sr));
                    }
                    return;
                }

                if (message.StartsWith("!READY"))
                {
                    StartReplication();
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Replicator: \"{0}\": {1}", message, e);
                Quit(e.ToString());
                return;
            }

            Quit("Unknown command: \"" + message + "\"");
            return;
        }

        // Must run on a SC thread
        private void BuildReplicationState()
        {
            _replicationStateQueue = new ConcurrentQueue<KeyValuePair<string, ulong>>();
            Db.Transact(() =>
            {
                foreach (Replication repl in Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE r.TableId LIKE ?", PeerTableIdPrefix + '%'))
                {
                    _replicationStateQueue.Enqueue(new KeyValuePair<string, ulong>(repl.TableId, repl.CommitId));
                }
            });
            return;
        }

        // Can be called from non-SC thread
        private void HandleSendReplicationStateResult(Task t)
        {
            if (t.IsFaulted || t.IsCanceled)
                _dbsess.RunAsync(() => { ProcessFailedSendResult(t); });
            else
                _dbsess.RunAsync(() => { SendReplicationState(); });
        }

        // Must run on a SC thread
        private void SendReplicationState()
        {
            if (_replicationStateQueue.IsEmpty)
            {
                _replicationStateQueue = null;
                _sender.SendStringAsync("!READY", CancellationToken).ContinueWith(HandleSendResult);
                return;
            }

            KeyValuePair<string, ulong> nextpair;
            var sb = new StringBuilder();
            sb.Append("!TPOS ");
            while (_replicationStateQueue.TryDequeue(out nextpair))
            {
                StringSerializer.Serialize(sb, StripDatabasePrefix(nextpair.Key));
                StringSerializer.Serialize(sb, nextpair.Value);
                if (sb.Length > 200)
                {
                    break;
                }
            }
            _sender.SendStringAsync(sb.ToString(), CancellationToken).ContinueWith(HandleSendReplicationStateResult);
            return;
        }

        // Must run on a SC thread
        public void Quit(string error = "")
        {
            if (!_isQuitting)
            {
                _isQuitting = true;
                _sender.SendStringAsync("!QUIT " + error, CancellationToken).ContinueWith((t1) => {
                    _dbsess.RunAsync(() => {
                        _sender.CloseAsync((error == "") ? 1000 : 4000, error, CancellationToken).ContinueWith((t2) =>
                        {
                            Dispose();
                        });
                    });
                });
            }
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
