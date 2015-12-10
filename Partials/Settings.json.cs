using System;
using System.Threading;
using Starcounter;
using Starcounter.Internal;
using Starcounter.TransactionLog;

namespace Replicator {
    partial class Settings : Partial, IBound<Configuration> {

        void Handle(Input.ParentUri Action)
        {
            this.Data.ParentUri = Action.Value;
            this.Data.ParentGuid = "";
            this.Transaction.Commit();
        }

        void Handle(Input.ReconnectMinimumWaitSeconds Action)
        {
            this.Data.ReconnectMinimumWaitSeconds = (int) Action.Value;
            this.Transaction.Commit();
        }

        void Handle(Input.ReconnectMaximumWaitSeconds Action)
        {
            this.Data.ReconnectMaximumWaitSeconds = (int)Action.Value;
            this.Transaction.Commit();
        }
    }
}
