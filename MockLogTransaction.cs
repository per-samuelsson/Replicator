using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Starcounter.TransactionLog;
using Newtonsoft.Json;

namespace Replicator
{
    class MockLogTransaction : ILogTransaction
    {
        public MockLogTransaction(Guid dbGuid, ulong commitID)
        {
            CommitID = commitID;
            DatabaseGuid = dbGuid;
        }

        public ulong CommitID
        {
            get;
            set;
        }

        public Guid DatabaseGuid
        {
            get;
            set;
        }

        public ulong GetCommitID()
        {
            return CommitID;
        }

        public Guid GetDatabaseGuid()
        {
            return DatabaseGuid;
        }

    }
}
