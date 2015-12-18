using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Starcounter;
using Starcounter.TransactionLog;

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
        // The GUID of the database
        public string DatabaseGuid;
        // and the last LogPosition we got from them
        public ulong Address;
        public ulong Signature;
        public ulong CommitId;
    }

    public sealed class Replicator : IDisposable
    {
        private bool _isServer;
        private bool _isConnected;
        private DbSession _dbsess;
        private IWebSocketSender _sender;
        private ILogManager _logmanager;
        private CancellationToken _ct;
        private Guid _selfGuid;
        private Guid _peerGuid;
        private string _peerGuidString;
        private ILogReader _reader = null;

        private LogApplicator _applicator = null;
        private SemaphoreSlim _inputSem = new SemaphoreSlim(1);
        private ConcurrentQueue<string> _input = new ConcurrentQueue<string>();
        private SemaphoreSlim _logQueueSem = new SemaphoreSlim(1);
        private ConcurrentQueue<Task<LogReadResult>> _logQueue = new ConcurrentQueue<Task<LogReadResult>>();
        private DateTime _lastEmptyTransactionSend = DateTime.Now;

        private HashSet<string> _filterTables = new HashSet<string>();
        private HashSet<string> _negativeCache = new HashSet<string>(); // filter URIs that have returned 404

        private JsonSerializerSettings _serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto };

        public Replicator(bool isServer, DbSession dbsess, IWebSocketSender sender, ILogManager manager, CancellationToken ct)
        {
            _isServer = isServer;
            _isConnected = false;
            _dbsess = dbsess;
            _sender = sender;
            _logmanager = manager;
            _ct = ct;
            _selfGuid = Program.GetDatabaseGuid();
            PeerGuid = Guid.Empty;
            _sender.SendStringAsync("!GUID " + _selfGuid.ToString(), _ct).ContinueWith(HandleSendResult);
            _applicator = new LogApplicator();
            _filterTables.Add("Replicator.Replication");
            _filterTables.Add("Replicator.Configuration");
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

        // Must run on a SC thread
        private void StartReplication(LogPosition pos)
        {
            if (!IsPeerGuidSet)
                throw new InvalidOperationException("peer GUID not set");
            _reader = _logmanager.OpenLog(Starcounter.Internal.StarcounterEnvironment.DatabaseNameLower, ReplicationParent.TransactionLogDirectory, pos);
            if (_reader != null)
            {
                if (_isServer)
                {
                    Db.Transact(() => {
                        Configuration conf = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", PeerGuidString).First;
                        if (conf == null)
                        {
                            Console.WriteLine("Did not find child GUID {0} in Replicator.Configuration", PeerGuidString);
                            conf = new Configuration()
                            {
                                DatabaseGuid = PeerGuidString,
                                ParentGuid = _selfGuid.ToString(),
                                ReconnectMinimumWaitSeconds = 1,
                                ReconnectMaximumWaitSeconds = 60 * 60 * 24,
                            };
                        }
                        else
                        {
                            conf.ParentGuid = _selfGuid.ToString();
                        }
                    });

                }
                _reader.ReadAsync(_ct).ContinueWith(HandleOutboundTransaction);
            }
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
            if (FilterTransaction(lrr))
            {
                _sender.SendStringAsync(JsonConvert.SerializeObject(lrr, _serializerSettings), _ct).ContinueWith(HandleSendResult);
            }
        }

        private bool FilterCreate(string baseUri, ref create_record_entry record)
        {
            bool retv = false; // block replication by default
            Response response;
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.GET(baseUri + PeerGuidString);
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    // 200 will allow replication unless POST forbids it
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<create_record_entry>(response.Body, _serializerSettings);
                    }
                }
            }
            baseUri += "create/";
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.POST(baseUri + PeerGuidString, JsonConvert.SerializeObject(record, _serializerSettings));
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<create_record_entry>(response.Body, _serializerSettings);
                    }
                }
                else
                {
                    // allow POST to override the GET
                    retv = false;
                }
            }
            return retv;
        }

        private bool FilterUpdate(string baseUri, ref update_record_entry record)
        {
            bool retv = false; // block replication by default
            Response response;
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.GET(baseUri + PeerGuidString);
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    // 200 will allow replication unless POST forbids it
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<update_record_entry>(response.Body, _serializerSettings);
                    }
                }
            }
            baseUri += "update/";
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.POST(baseUri + PeerGuidString, JsonConvert.SerializeObject(record, _serializerSettings));
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<update_record_entry>(response.Body, _serializerSettings);
                    }
                }
                else
                {
                    // allow POST to override the GET
                    retv = false;
                }
            }
            return retv;
        }

        private bool FilterDelete(string baseUri, ref delete_record_entry record)
        {
            bool retv = false; // block replication by default
            Response response;
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.GET(baseUri + PeerGuidString);
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    // 200 will allow replication unless POST forbids it
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<delete_record_entry>(response.Body, _serializerSettings);
                    }
                }
            }
            baseUri += "delete/";
            if (!_negativeCache.Contains(baseUri))
            {
                response = Self.POST(baseUri + PeerGuidString, JsonConvert.SerializeObject(record, _serializerSettings));
                if (response == null || response.StatusCode == 404)
                {
                    _negativeCache.Add(baseUri);
                }
                else if (response.StatusCode == 200)
                {
                    retv = true;
                    if (response.Body != null)
                    {
                        record = JsonConvert.DeserializeObject<delete_record_entry>(response.Body, _serializerSettings);
                    }
                }
                else
                {
                    // allow POST to override the GET
                    retv = false;
                }
            }
            return retv;
        }

        // Must run on a SC thread
        private bool FilterTransaction(LogReadResult lrr)
        {
            try
            {
                var tran = lrr.transaction_data;
                int index;

                lock (_negativeCache)
                {
                    index = 0;
                    while (index < tran.creates.Count)
                    {
                        var record = tran.creates[index];
                        if (record.table == "Replicator.Replication")
                        {
                            for (int i = 0; i < record.columns.Length; i++)
                            {
                                if (record.columns[i].name == "DatabaseGuid")
                                {
                                    if ((string)record.columns[i].value == PeerGuidString)
                                        return false;
                                }
                            }
                            tran.creates.RemoveAt(index);
                        }
                        else if (FilterCreate("/Replicator/out/" + tran.creates[index].table + "/", ref record))
                        {
                            tran.creates[index] = record;
                            index++;
                        }
                        else
                        {
                            tran.creates.RemoveAt(index);
                        }
                    }

                    index = 0;
                    while (index < tran.updates.Count)
                    {
                        var record = tran.updates[index];
                        if (record.table == "Replicator.Replication")
                        {
                            for (int i = 0; i < record.columns.Length; i++)
                            {
                                if (record.columns[i].name == "DatabaseGuid")
                                {
                                    if ((string)record.columns[i].value == PeerGuidString)
                                        return false;
                                }
                            }
                            tran.updates.RemoveAt(index);
                        }
                        else if (FilterUpdate("/Replicator/out/" + tran.updates[index].table + "/", ref record))
                        {
                            tran.updates[index] = record;
                            index++;
                        }
                        else
                        {
                            tran.updates.RemoveAt(index);
                        }
                    }

                    index = 0;
                    while (index < tran.deletes.Count)
                    {
                        var record = tran.deletes[index];
                        if (record.table == "Replicator.Replication")
                        {
                            tran.deletes.RemoveAt(index);
                        }
                        else if (FilterDelete("/Replicator/out/" + tran.deletes[index].table + "/", ref record))
                        {
                            tran.deletes[index] = record;
                            index++;
                        }
                        else
                        {
                            tran.deletes.RemoveAt(index);
                        }
                    }
                }

                if (tran.updates.Count > 0 || tran.creates.Count > 0 || tran.deletes.Count > 0)
                    return true;

                // send empty transactions at regular intervals in order to update log position
                if (DateTime.Now.Subtract(_lastEmptyTransactionSend).Seconds > 10)
                {
                    _lastEmptyTransactionSend = DateTime.Now;
                    return true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Replicator: {0}: FilterTransaction {1}: {2}", PeerGuidString, lrr.continuation_position, e);
            }

            // Transaction is empty or exception occured, don't send it
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

                    LogReadResult tran = JsonConvert.DeserializeObject<LogReadResult>(message, _serializerSettings);
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
                    PeerGuid = peerGuid;
                    var reply = "!LPOS " + JsonConvert.SerializeObject(LastLogPosition, _serializerSettings);
                    _sender.SendStringAsync(reply, _ct).ContinueWith(HandleSendResult);
                    return;
                }

                if (message.StartsWith("!LPOS "))
                {
                    StartReplication(JsonConvert.DeserializeObject<LogPosition>(message.Substring(6), _serializerSettings));
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
        public void Quit(string error = "")
        {
            _sender.SendStringAsync("!QUIT " + error, _ct).ContinueWith((t1) => {
                _dbsess.RunAsync(() => {
                    _sender.CloseAsync((error == "") ? 1000 : 4000, error, _ct).ContinueWith((t2) =>
                    {
                        Dispose();
                    });
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
