﻿using System;
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
    class Program
    {
        static ReplicationChild _client = null;
        static ReplicationParent _server = null;
        static CancellationTokenSource _cts = new CancellationTokenSource();
        static ILogManager _servermanager = new MockLogManager();
        static ILogManager _clientmanager = new MockLogManager();

        static void Main()
        {
            _server = new ReplicationParent(_servermanager, _cts.Token);
            _client = new ReplicationChild(_clientmanager, System.Environment.MachineName, StarcounterEnvironment.Default.UserHttpPort, _cts.Token);
        }
    }
}
