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
        private DbSession _dbsess;
        private IWebSocketSender _sender;
        private Guid _selfGuid = Db.Environment.DatabaseGuid;
        private Guid _peerGuid = Guid.Empty;
        private LogApplicator _applicator = new LogApplicator();
        private ulong _transactionsProcessed = 0;
        private bool _isQuitting = false;
        private bool _isOk = false;

        private ConcurrentQueue<KeyValuePair<string, ulong>> _replicationStateQueue = null;
        private Dictionary<string, ulong> _peerTablePositions = null;
        private HashSet<string>[] _tableFilters = null;

        // used to synchronize websocket input to the replicator
        private SemaphoreSlim _inputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _input = new ConcurrentQueue<string>();

        private FilteredLogReader[] _readers = null;
        private Task<LogReadResult>[] _readerTasks = null;
        private ConcurrentQueue<Task<LogReadResult>> _logQueue = new ConcurrentQueue<Task<LogReadResult>>();
        private SemaphoreSlim _logQueueSem = new SemaphoreSlim(1);

        public Replicator(DbSession dbsess, IWebSocketSender sender, ILogManager manager, CancellationToken ct, Dictionary<string, int> tablePrios = null)
        {
            _dbsess = dbsess;
            _sender = sender;
            LogManager = manager;
            CancellationToken = ct;
            if (tablePrios != null)
                BuildFilters(tablePrios);
            _sender.SendStringAsync("!GUID " + _selfGuid.ToString(), CancellationToken).ContinueWith(HandleSendResult);
        }

        private void BuildFilters(Dictionary<string, int> tablePrios)
        {
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

        public ulong TransactionsProcessed
        {
            get { return _transactionsProcessed; }
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

        private void InitializeReader(int index)
        {
            var reader = new FilteredLogReader(this, _peerTablePositions, _tableFilters == null ? null : _tableFilters[index]);
            _readers[index] = reader;
            _readerTasks[index] = reader.ReadAsync(CancellationToken);
        }

        // Must run on a SC thread
        private void StartReplication()
        {
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

            ReadAsync(CancellationToken).ContinueWith(HandleOutboundTransaction);
        }

        // Must run on a SC thread
        private async Task<LogReadResult> ReadAsync(CancellationToken ct)
        {
            if (ct.IsCancellationRequested)
                return null;

            await Task.WhenAny<LogReadResult>(_readerTasks);

            // we process array in reverse order to get higher prio stuff done first
            for (int i = _readerTasks.Length - 1; i >= 0; i--)
            {
                var t = _readerTasks[i];
                if (t.IsCompleted)
                {
                    _readerTasks[i] = _readers[i].ReadAsync(ct);
                    if (t.IsFaulted)
                    {
                        ExceptionDispatchInfo.Capture(t.Exception.InnerException).Throw();
                    }
                    if (t.IsCanceled)
                    {
                        continue;
                    }
                    return t.Result;
                }
            }

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
                }
                catch (Exception e)
                {
                    Console.WriteLine("Replicator: {0}: ProcessLogQueue: {1}", PeerGuidString, e);
                }
                finally
                {
                    if (!CancellationToken.IsCancellationRequested)
                        ReadAsync(CancellationToken).ContinueWith(HandleOutboundTransaction);
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

        public bool IsOk
        {
            get { return _isOk; }
        }

        // Must run on a SC thread
        private void ProcessIncomingTransaction(LogReadResult tran)
        {
            try
            {
                Db.Transact(() =>
                {
                    ulong commitId = tran.continuation_position.commit_id;
                    if (_tableFilters == null)
                    {
                        UpdateCommitId("", commitId);
                    }
                    else
                    {
                        ForAllTablesInTransaction(tran, (tableName) => UpdateCommitId(tableName, commitId));
                    }
                    _applicator.Apply(tran.transaction_data);
                });
                _transactionsProcessed++;
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
                throw;
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
                    if (!_isOk && !_isQuitting)
                    {
                        _isOk = true;
                        _sender.SendStringAsync("!OK", CancellationToken).ContinueWith(HandleSendResult);
                    }
                    return;
                }


                // Command processing

                if (message.StartsWith("!OK"))
                {
                    _isOk = true;
                    return;
                }

                if (message.StartsWith("!QUIT"))
                {
                    _isQuitting = true;
                    QuitMessage = message.Substring(5).Trim();
                    // Sender of !QUIT will close the connection
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

                int firstNewLine = error.IndexOf('\n');
                string closeStatus = (firstNewLine < 0) ? error : error.Substring(0, firstNewLine);
                if (closeStatus.Length > 25)
                {
                    if (closeStatus.Length > 122)
                    {
                        closeStatus = closeStatus.Substring(0, 123);
                    }
                    while (Encoding.UTF8.GetByteCount(closeStatus) > 123)
                    {
                        closeStatus = closeStatus.Substring(0, closeStatus.Length - 1);
                    }
                }

                // _sender.SendStringAsync("!QUIT " + error, CancellationToken).ContinueWith(HandleSendResult);
                _sender.SendStringAsync("!QUIT " + error, CancellationToken).ContinueWith((t1) => {
                    _dbsess.RunAsync(() => {
                        _sender.CloseAsync((error == "") ? 1000 : 4000, closeStatus, CancellationToken).ContinueWith((t2) =>
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
