using System;
using System.Threading;
using System.Collections.Generic;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace Replicator
{
    [Database]
    public class Configuration
    {
        public string DatabaseGuid;
        public string ParentUri;
        public string ParentGuid;
        public int ReconnectMinimumWaitSeconds;
        public int ReconnectMaximumWaitSeconds;
    }

    public class ParentStatus
    {
        private const int MaxMessages = 100;
        public string DatabaseGuid = "";
        public List<string> Messages = new List<string>();

        public string Message
        {
            get
            {
                lock (Messages)
                {
                    if (Messages.Count < 1)
                        return "";
                    return Messages[Messages.Count - 1];
                }
            }
            set
            {
                lock (Messages)
                {
                    Messages.Add(value);
                    while (Messages.Count > MaxMessages)
                    {
                        Messages.RemoveAt(0);
                    }
                }
            }
        }

        public bool IsConnected
        {
            get { return DatabaseGuid != ""; }
        }
    }

    public class Program
    {
        public const string ReplicatorServicePath = "/Replicator/service";
        public const string ReplicatorWebsocketProtocol = "sc-replicator";
        private static ReplicationParent _server = null;
        private static ReplicationChild _client = null;
        private static ILogManager _servermanager = new MockLogManager();
        private static ILogManager _clientmanager = new MockLogManager();
        private static CancellationTokenSource _cts = new CancellationTokenSource();
        private static ParentStatus _parentStatus = new ParentStatus();

        static public Guid GetDatabaseGuid()
        {
            // generate a fake database GUID until @bigwad exports the kernel API
            return new Guid(
                System.Security.Cryptography.MD5.Create().ComputeHash(
                    System.Text.Encoding.ASCII.GetBytes(
                        System.Environment.MachineName + "/" + StarcounterEnvironment.DatabaseNameLower
                        )
                    )
                );
        }

        static private Configuration GetConfiguration()
        {
            Configuration conf = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", GetDatabaseGuid().ToString()).First;
            if (conf == null)
            {
                conf = new Configuration()
                {
                    DatabaseGuid = GetDatabaseGuid().ToString(),
                    ParentUri = System.Environment.MachineName + ":" + StarcounterEnvironment.Default.UserHttpPort,
                    ParentGuid = "",
                    ReconnectMinimumWaitSeconds = 1,
                    ReconnectMaximumWaitSeconds = 60 * 60 * 24,
                };
            }
            return conf;
        }

        static public ParentStatus ParentStatus
        {
            get { return _parentStatus; }
        }

        static public int ReconnectMinimumWaitSeconds
        {
            get
            {
                int n = 1;
                int maximum = 1;
                Db.Transact(() => {
                    maximum = GetConfiguration().ReconnectMaximumWaitSeconds;
                    n = GetConfiguration().ReconnectMinimumWaitSeconds;
                });
                if (maximum < 1)
                    maximum = 1;
                if (n > maximum)
                    return maximum;
                return n;
            }
            set
            {
                Db.Transact(() => {
                    GetConfiguration().ReconnectMinimumWaitSeconds = value;
                });
            }
        }

        static public int ReconnectMaximumWaitSeconds
        {
            get
            {
                int n = 60 * 60 * 24;
                int minimum = 1;
                Db.Transact(() => {
                    minimum = GetConfiguration().ReconnectMinimumWaitSeconds;
                    n = GetConfiguration().ReconnectMaximumWaitSeconds;
                });
                if (minimum < 1)
                    minimum = 1;
                if (n < minimum)
                    return minimum;
                return n;
            }
            set
            {
                Db.Transact(() => {
                    GetConfiguration().ReconnectMaximumWaitSeconds = value;
                });
            }
        }

        static public string ParentUri
        {
            get
            {
                string uri = "";
                Db.Transact(() => {
                    uri = GetConfiguration().ParentUri;
                });
                return uri;
            }
            set
            {
                Db.Transact(() => {
                    var conf = GetConfiguration();
                    conf.ParentUri = value;
                    conf.ParentGuid = "";
                });
            }
        }

        static public string ParentGuid
        {
            get
            {
                string uri = "";
                Db.Transact(() => {
                    uri = GetConfiguration().ParentGuid;
                });
                return uri;
            }
            set
            {
                new DbSession().RunAsync(() =>
                {
                    Db.Transact(() =>
                    {
                        GetConfiguration().ParentGuid = value;
                    });
                }, 0);
            }
        }

        static public string Status
        {
            get
            {
                return _parentStatus.Message;
            }
            set
            {
                new DbSession().RunAsync(() =>
                {
                    _parentStatus.Message = value == null ? "" : value;
                    Session.ForAll((s) =>
                    {
                        s.CalculatePatchAndPushOnWebSocket();
                    });
                });
            }
        }

        static public void Connect()
        {
            if (_client != null)
            {
                _cts.Cancel();
                _cts = new CancellationTokenSource();
            }
            _client = new ReplicationChild(_clientmanager, Program.ParentUri, _cts.Token);
        }

        static void Main()
        {
            Status = "Not connected.";
            new HttpHandlers();
            _server = new ReplicationParent(_servermanager, _cts.Token);
        }
    }
}
