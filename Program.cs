using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Net.WebSockets;
using System.Threading;
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
        public string Status;
    }

    public class Program
    {
        public const string ReplicatorServicePath = "/Replicator/service";
        public const string ReplicatorWebsocketProtocol = "sc-replicator";
        static Guid _selfGuid = Guid.NewGuid();
        static ReplicationParent _server = null;
        static ReplicationChild _client = null;
        static ILogManager _servermanager = new MockLogManager();
        static ILogManager _clientmanager = new MockLogManager();
        static CancellationTokenSource _cts = new CancellationTokenSource();

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
            Configuration conf = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", Program.GetDatabaseGuid().ToString()).First;
            if (conf == null)
            {
                conf = new Configuration()
                {
                    DatabaseGuid = GetDatabaseGuid().ToString(),
                    ParentUri = System.Environment.MachineName + ":" + StarcounterEnvironment.Default.UserHttpPort,
                    ParentGuid = "",
                    ReconnectMinimumWaitSeconds = 1,
                    ReconnectMaximumWaitSeconds = 60 * 60 * 24,
                    Status = "",
                };
            }
            return conf;
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
                string s = "";
                Db.Transact(() => {
                    s = GetConfiguration().Status;
                });
                return s;
            }
            set
            {
                new DbSession().RunAsync(() =>
                {
                    if (value == null)
                        value = "";
                    Db.Transact(() => {
                        GetConfiguration().Status = value;
                    });
                    Session.ForAll((s) =>
                    {
                        s.CalculatePatchAndPushOnWebSocket();
                    });
                }, 0);
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
