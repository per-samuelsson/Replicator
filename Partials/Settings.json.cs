using System;
using System.Threading;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace Replicator {
    partial class Settings : Partial {
        static CancellationTokenSource _cts = new CancellationTokenSource();
        static ReplicationChild _client = null;
        static ILogManager _clientmanager = new MockLogManager();

        public string ParentAddress = "";

        void Handle(Input.SourceIp Action) {
            ParentAddress = Action.Value;
            Console.WriteLine("SourceIp changed: " + Action.Value);
        }

        void Handle(Input.Save Action) {
            _client = new ReplicationChild(_clientmanager, ParentAddress, StarcounterEnvironment.Default.UserHttpPort, _cts.Token);
            Console.WriteLine("Save clicked");
        }
    }
}
