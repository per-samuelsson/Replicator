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
        public ulong CommitID()
        {
            return 0;
        }

        public Guid DatabaseGuid()
        {
            return new Guid();
        }

        public string Serialize()
        {
            return JsonConvert.SerializeObject(this);
        }

        public void Deserialize(string data)
        {

        }
    }
}
