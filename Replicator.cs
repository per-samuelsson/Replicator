using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Starcounter;
using Starcounter.TransactionLog;
using System.IO;
using System.Text;
using System.Runtime.ExceptionServices;

namespace Replicator
{
    [Database]
    public class Replication
    {
        // The GUID of the database + ':' + table name
        public string TableId;
        // and the last LogPosition we got from them for that table
        public ulong CommitId;
    }

    public sealed class Replicator : IDisposable, IReplicatorState
    {
        private DbSession _dbsess;
        private IWebSocketSender _sender;
        private LogApplicator _applicator = new LogApplicator();

        // private ConcurrentQueue<KeyValuePair<string, ulong>> _replicationStateQueue = null;
        private Dictionary<string, ulong> _peerTablePositions = null;
        private List<HashSet<string>> _peerTableFilters = null;
        private HashSet<string>[] _tableFilters = null;

        // used to synchronize websocket output for the replicator
        private SemaphoreSlim _outputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _output = new ConcurrentQueue<string>();

        // used to synchronize websocket input to the replicator
        private SemaphoreSlim _inputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _input = new ConcurrentQueue<string>();

        private FilteredLogReader[] _readers = null;
        private Task<LogReadResult>[] _readerTasks = null;
        private ConcurrentQueue<Task<LogReadResult>> _logQueue = new ConcurrentQueue<Task<LogReadResult>>();
        private SemaphoreSlim _logQueueSem = new SemaphoreSlim(1);

        private Guid _peerGuid = Guid.Empty;

        static public bool IsTransactionEmpty(TransactionData tran)
        {
            return tran.updates.Count == 0 && tran.creates.Count == 0 && tran.deletes.Count == 0;
        }

        private Guid SelfGuid
        {
            get
            {
                return Db.Environment.DatabaseGuid;
            }
        }

        public bool IsQuitting
        {
            get;
            private set;
        }

        public RunState RunState
        {
            get;
            private set;
        }

        public ulong TransactionsReceived
        {
            get;
            private set;
        }

        public ulong TransactionsSent
        {
            get;
            private set;
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

        public bool PeerHasFilters
        {
            get { return _peerTableFilters != null; }
        }


        public Replicator(DbSession dbsess, IWebSocketSender sender, ILogManager manager, CancellationToken ct, Dictionary<string, int> tablePrios = null)
        {
            _dbsess = dbsess;
            _sender = sender;
            LogManager = manager;
            CancellationToken = ct;
            if (tablePrios != null)
            {
                BuildFilters(tablePrios);
            }
            RunState = RunState.Starting;
            var sb = new StringBuilder();
            sb.Append("!ID ");
            StringSerializer.Serialize(sb, Db.Environment.DatabaseName);
            StringSerializer.Serialize(sb, SelfGuid.ToString());
            Send(sb);
        }

        private void Send(StringBuilder sb)
        {
            if (sb.Length > 0)
            {
                Send(sb.ToString());
                sb.Clear();
            }
        }

        private void Send(string s)
        {
            _output.Enqueue(s);
            ProcessOutput();
        }

        private void BuildFilters(Dictionary<string, int> tablePrios)
        {
            if (tablePrios.Count == 0)
            {
                throw new ArgumentException("tablePrios is empty (and not null)");
            }
            var sortedTableFilters = new SortedDictionary<int, HashSet<string>>();
            foreach (var kv in tablePrios)
            {
                HashSet<string> tableSet;
                if (!sortedTableFilters.TryGetValue(kv.Value, out tableSet))
                {
                    tableSet = new HashSet<string>();
                    sortedTableFilters[kv.Value] = tableSet;
                }
                tableSet.Add(kv.Key);
            }

            int index = 0;
            _tableFilters = new HashSet<string>[sortedTableFilters.Count];
            foreach (var kv in sortedTableFilters)
            {
                _tableFilters[index] = kv.Value;
                index++;
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
        public void ProcessOutput()
        {
            if (_outputSem.Wait(0))
            {
                try
                {
                    string message;
                    if (_output.TryDequeue(out message))
                    {
                        _sender.SendStringAsync(message, CancellationToken.None).ContinueWith(HandleSendResult);
                        return;
                    }
                    if (IsQuitting)
                    {
                        _sender.CloseAsync(1000, string.Empty, CancellationToken.None).ContinueWith(HandleCloseResult);
                        return;
                    }
                }
                catch (Exception)
                {
                    _outputSem.Release();
                    throw;
                }
                _outputSem.Release();
            }
        }

        // Can be called from non-SC thread
        private void HandleCloseResult(Task t)
        {
            _outputSem.Release();
            Dispose();
            return;
        }

        // Can be called from non-SC thread
        private void HandleSendResult(Task t)
        {
            _outputSem.Release();
            if (t.IsFaulted || t.IsCanceled)
            {
                _dbsess.RunAsync(() => { ProcessFailedSendResult(t); });
            }
            else
            {
                _dbsess.RunAsync(ProcessOutput);
            }
            return;
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

        public string PeerDatabaseName
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

            private set
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

        // Must run on a SC thread
        private void ProcessFailedSendResult(Task t)
        {
            if (t.IsFaulted)
            {
                Quit(t.Exception);
                return;
            }

            if (t.IsCanceled)
            {
                Canceled();
                return;
            }
        }

        private void InitializeReader(int index)
        {
            var reader = new FilteredLogReader(this, _peerTablePositions, _tableFilters == null ? null : _tableFilters[index]);
            _readers[index] = reader;
            _readerTasks[index] = reader.ReadAsync(CancellationToken);
        }

        // Must run on a SC thread
        private void StartReplication()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");

            if (_tableFilters == null)
            {
                _readers = new FilteredLogReader[1];
                _readerTasks = new Task<LogReadResult>[1];
            }
            else
            {
                _readers = new FilteredLogReader[_tableFilters.Length];
                _readerTasks = new Task<LogReadResult>[_tableFilters.Length];
            }

            for (int i = _readers.Length - 1; i >= 0; i--)
            {
                InitializeReader(i);
            }

            ReadAsync().ContinueWith(HandleOutboundTransaction);
        }

        private async Task<LogReadResult> ReadAsync()
        {
            while (!CancellationToken.IsCancellationRequested)
            {
                // we process array in reverse order to get higher prio stuff done first
                for (int i = _readerTasks.Length - 1; i >= 0; i--)
                {
                    var t = _readerTasks[i];
                    if (t.IsCompleted)
                    {
                        _readerTasks[i] = _readers[i].ReadAsync(CancellationToken);
                        if (t.IsFaulted)
                        {
                            ExceptionDispatchInfo.Capture(t.Exception.InnerException).Throw();
                        }
                        if (t.IsCanceled)
                        {
                            Canceled();
                        }
                        return t.Result;
                    }
                }

                await Task.WhenAny(_readerTasks);
            }

            Canceled();
            return null;
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
                    if (!CancellationToken.IsCancellationRequested)
                        ReadAsync().ContinueWith(HandleOutboundTransaction);
                }
                finally
                {
                    _logQueueSem.Release();
                }
            }
        }

        // Must run on a SC thread
        private void ProcessOutboundTransaction(Task<LogReadResult> t)
        {
            if (t.IsFaulted)
            {
                Quit(t.Exception);
                return;
            }

            if (t.IsCanceled || CancellationToken.IsCancellationRequested)
            {
                Canceled();
                return;
            }

            Send(StringSerializer.Serialize(new StringBuilder(), t.Result).ToString());
        }

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
        private void ProcessIncomingTransaction(LogReadResult lrr)
        {
            try
            {
                Db.Transact(() =>
                {
                    ulong commitId = lrr.continuation_position.commit_id;
                    if (PeerHasFilters)
                    {
                        ForAllTablesInTransaction(lrr, (tableName) => UpdateCommitId(tableName, commitId));
                    }
                    else
                    {
                        UpdateCommitId(string.Empty, commitId);
                    }
                    _applicator.Apply(lrr.transaction_data);
                });
                TransactionsReceived++;
            }
            catch (Exception e)
            {
                uint code;
                // Special handling for ScErrTableNotFound (SCERR4230) and ScErrSchemaCodeMismatch (SCERR4177)
                if (ErrorCode.TryGetCode(e, out code))
                {
                    if (code == 4230 || code == 4177)
                    {
                        // bisect transaction with current loaded classes to find which ones are missing
                        var tableset = new HashSet<string>();
                        ForAllTablesInTransaction(lrr, (tableName) => tableset.Add(tableName));
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
                throw;
            }
        }

        // Must run on a SC thread
        private void ProcessStringMessage(string message)
        {
            if (IsQuitting)
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
                    if (TransactionsReceived == 1 && !IsQuitting)
                    {
                        Send("!OK");
                    }
                    return;
                }


                // Command processing

                if (message.StartsWith("!OK") && RunState == RunState.Starting)
                {
                    RunState = RunState.Running;
                    return;
                }

                if (message.StartsWith("!QUIT"))
                {
                    IsQuitting = true;
                    QuitMessage = message.Substring(5).Trim();
                    // Sender of !QUIT will close the connection
                    return;
                }

                if (message.StartsWith("!ID "))
                {
                    var sr = new StringReader(message);
                    for (int i = 0; i < 4; i++) sr.Read();
                    var peerDatabaseName = StringSerializer.DeserializeString(sr);
                    var peerGuid = Guid.Parse(StringSerializer.DeserializeString(sr));
                    if (peerGuid == Guid.Empty)
                    {
                        Quit("GUID is empty");
                        return;
                    }
                    if (peerGuid == SelfGuid)
                    {
                        Quit("GUID is my own");
                        return;
                    }
                    PeerDatabaseName = peerDatabaseName;
                    PeerGuid = peerGuid;
                    SendFilterState();
                    BuildReplicationState();
                    // SendReplicationState();
                    return;
                }

                if (message.StartsWith("!FILTER "))
                {
                    var sr = new StringReader(message);
                    for (int i = 0; i < 8; i++) sr.Read();
                    var tableSet = new HashSet<string>();
                    while (StringSerializer.SkipWS(sr) != -1)
                    {
                        tableSet.Add(StringSerializer.DeserializeString(sr));
                    }
                    if (tableSet.Count > 0)
                    {
                        if (_peerTableFilters == null)
                        {
                            _peerTableFilters = new List<HashSet<string>>();
                        }
                        _peerTableFilters.Add(tableSet);
                    }
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
                Quit(e);
                return;
            }

            Quit("Unknown command: \"" + message + "\"");
            return;
        }

        // Must run on a SC thread
        private void SendFilterState()
        {
            if (_tableFilters != null)
            {
                for (int i = 0; i < _tableFilters.Length; i++)
                {
                    var sb = new StringBuilder();
                    sb.Append("!FILTER ");
                    foreach (var tableName in _tableFilters[i])
                    {
                        StringSerializer.Serialize(sb, tableName);
                    }
                    Send(sb);
                }
            }
        }

        // Must run on a SC thread
        private void BuildReplicationState()
        {
            // _replicationStateQueue = new ConcurrentQueue<KeyValuePair<string, ulong>>();
            Db.Transact(() =>
            {
                ulong databaseCommitId = 0;

                Replication dbRepl = Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE r.TableId = ?", PeerTableIdPrefix).First;
                if (dbRepl != null)
                {
                    databaseCommitId = dbRepl.CommitId;
                }

                var sb = new StringBuilder();
                foreach (Replication repl in Db.SQL<Replication>("SELECT r FROM Replicator.Replication r WHERE r.TableId LIKE ?", PeerTableIdPrefix + '%'))
                {
                    // remove table entries that are obsolete
                    if (repl.TableId != PeerTableIdPrefix && repl.CommitId <= databaseCommitId)
                    {
                        repl.Delete();
                    }
                    else
                    {
                        if (sb.Length == 0)
                            sb.Append("!TPOS ");
                        StringSerializer.Serialize(sb, StripDatabasePrefix(repl.TableId));
                        StringSerializer.Serialize(sb, repl.CommitId);
                        if (sb.Length > 200)
                        {
                            Send(sb);
                        }
                        // _replicationStateQueue.Enqueue(new KeyValuePair<string, ulong>(repl.TableId, repl.CommitId));
                    }
                }
                Send(sb);
                Send("!READY");
            });
            return;
        }

        // Must run on a SC thread
        public void Canceled()
        {
            Quit("Canceled");
        }

        // Must run on a SC thread
        public void Quit(Exception e)
        {
            Quit(e.ToString());
            return;
        }

        // Must run on a SC thread
        public void Quit(string error = "")
        {
            if (!IsQuitting)
            {
                IsQuitting = true;
                Send("!QUIT " + error);
            }
        }

        public void Dispose()
        {
            if (!IsDisposed)
            {
                IsDisposed = true;
                PeerGuid = Guid.Empty;
                _sender.Dispose();
            }
        }
    }
}
