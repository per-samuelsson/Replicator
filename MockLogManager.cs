using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Starcounter.TransactionLog;

namespace Replicator
{
    public class MockLogManager : ILogManager
    {
        private Guid _selfGuid = Guid.NewGuid();

        public Guid GetDatabaseGuid()
        {
            return _selfGuid;
        }

        public ILogReader OpenLog(string path)
        {
            return null;
        }

        public ILogReader OpenLog(string path, LogPosition position)
        {
            return new MockLogReader(_selfGuid, position);
        }
    }
}
