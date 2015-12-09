using System;
using System.Threading;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace Replicator {
    partial class Settings : Partial {

        void Handle(Input.ParentUri Action)
        {
            Program.ParentUri = Action.Value;
        }

        void Handle(Input.ReconnectMinimumWaitSeconds Action)
        {
            Program.ReconnectMinimumWaitSeconds = (int) Action.Value;
        }

        void Handle(Input.ReconnectMaximumWaitSeconds Action)
        {
            Program.ReconnectMaximumWaitSeconds = (int) Action.Value;
        }

        void Handle(Input.ConnectNow Action) {
            Program.Connect();
        }
    }
}
