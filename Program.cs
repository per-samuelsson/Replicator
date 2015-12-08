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
    }

    class Program
    {
        static ILogManager _servermanager = new MockLogManager();
        static ReplicationParent _server = null;
        static CancellationTokenSource _cts = new CancellationTokenSource();

        static Guid GetDatabaseGuid()
        {
            return _servermanager.GetDatabaseGuid();
        }

        static void Main()
        {
            Db.Transact(() => {
                Configuration conf = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", Guid.Empty.ToString()).First;
                if (conf == null)
                {
                    conf = new Configuration()
                    {
                        DatabaseGuid = Guid.Empty.ToString(),
                        ParentUri = System.Environment.MachineName + ":" + StarcounterEnvironment.Default.UserHttpPort,
                    };
                }
            });

            new HttpHandlers();
            _server = new ReplicationParent(_servermanager, _cts.Token);
        }
    }
}
