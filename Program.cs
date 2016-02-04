using System;
using System.Threading;
using System.Collections.Generic;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;
using System.Collections.Concurrent;

namespace LogStreamer
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
                    string message = value == null ? "" : value;
                    if (Messages.Count < 1 || Messages[Messages.Count - 1] != message)
                    {
                        Messages.Add(message);
                        while (Messages.Count > MaxMessages)
                        {
                            Messages.RemoveAt(0);
                        }
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
        public const string LogStreamerServicePath = "/LogStreamer/service";
        public const string LogStreamerWebsocketProtocol = "sc-logstreamer";
        private static LogStreamerParent _server = null;
        private static LogStreamerChild _client = null;
        private static ILogManager _servermanager = new LogManager();
        private static ILogManager _clientmanager = new LogManager();
        private static CancellationTokenSource _serverCts = new CancellationTokenSource();
        private static CancellationTokenSource _clientCts = new CancellationTokenSource();
        private static ParentStatus _parentStatus = new ParentStatus();
        private static bool _streamingEnabled = false;

        public static HashSet<string> MySessions = new HashSet<string>();

        static private Configuration GetConfiguration()
        {
            Configuration conf = Db.SQL<Configuration>("SELECT c FROM LogStreamer.Configuration c WHERE c.DatabaseGuid = ?", Db.Environment.DatabaseGuid.ToString()).First;
            if (conf == null)
            {
                conf = new Configuration()
                {
                    DatabaseGuid = Db.Environment.DatabaseGuid.ToString(),
                    ParentUri = System.Environment.MachineName + ":" + StarcounterEnvironment.Default.UserHttpPort,
                    ParentGuid = "",
                    ReconnectMinimumWaitSeconds = 1,
                    ReconnectMaximumWaitSeconds = 60 * 60 * 24,
                };
            }
            return conf;
        }

        static public bool StreamingEnabled
        {
            get
            {
                return _streamingEnabled;
            }
            set
            {
                if (value)
                {
                    Program.Connect();
                }
                else
                {
                    Program.Disconnect();
                }
                RefreshSessions();
            }
        }

        public static string ConfiguredDatabaseKeyRangeString
        {
            get
            {
                var env = Db.Environment;
                return string.Format("{0}-{1}", env.FirstUserOid, env.LastUserOid);
            }
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
                RefreshSessions();
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
                RefreshSessions();
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
                RefreshSessions();
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
        }

        static public string Status
        {
            get
            {
                return _parentStatus.Message;
            }
            set
            {
                _parentStatus.Message = value == null ? "" : value;
                RefreshSessions();
            }
        }

        static private void RefreshSessions()
        {
            lock (MySessions)
            {
                foreach (var s in MySessions)
                {
                    Session.ScheduleTask(s, (session, sessionString) =>
                    {
                        if (session == null)
                        {
                            lock (MySessions)
                            {
                                MySessions.Remove(sessionString);
                            }
                        }
                        else
                        {
                            session.CalculatePatchAndPushOnWebSocket();
                        }
                    });
                }
            }
        }

        static public void Connect()
        {
            Disconnect();
            _streamingEnabled = true;
            _client = new LogStreamerChild(_clientmanager, ParentUri, _clientCts.Token, Whitelist);
        }

        static public void Disconnect()
        {
            if (_client != null)
            {
                _clientCts.Cancel();
                _clientCts = new CancellationTokenSource();
                _client = null;
            }
            _streamingEnabled = false;
            Status = "Not connected.";
        }

        static private Dictionary<string, int> Whitelist
        {
            get
            {
                /*
                // For testing, generate random filters
                var r = new Random();
                var d = new Dictionary<string, int>();
                if (r.Next(100) < 50)
                    d["Invoice"] = r.Next(2);
                if (r.Next(100) < 50)
                    d["InvoiceRow"] = r.Next(2);
                return d.Count > 0 ? d : null;
                */
                return null;
            }
        }


        static void Main(string[] args)
        {
            Console.WriteLine("Starting LogStreamer in {0}. Configured key range: {1}", 
                Db.Environment.DatabaseName, 
                ConfiguredDatabaseKeyRangeString
                );

            Db.Transact(() => { GetConfiguration(); }); // ensure that configuration object is created
            Status = "Not connected.";
            new HttpHandlers();
            _server = new LogStreamerParent(_servermanager, _serverCts.Token, Whitelist);

            foreach (var arg in args)
            {
                if (arg.Equals("@enabled", StringComparison.InvariantCultureIgnoreCase))
                {
                    // Enable the LogStreamer. This will effectively connect
                    // to any configured parent.
                    Program.StreamingEnabled = true;
                }
                else if (false)
                {
                    // Support "--parent=ip:port"?
                    // TODO:
                }
                else
                {
                    Console.WriteLine("Warning: Ignoring unrecognized argument '{0}'", arg);
                }
            }
        }
    }
}
