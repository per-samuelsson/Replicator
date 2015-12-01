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

namespace Replicator
{
    class Program
    {
        static ReplicationSink _client = null;
        static ReplicationSource _server = null;
        static CancellationTokenSource _cts = new CancellationTokenSource();

        static void Main()
        {
            _server = new ReplicationSource(null, _cts.Token);
        }
    }
}