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
        static Guid _selfGuid = Guid.NewGuid();

        public Guid GetDatabaseGuid()
        {
            return _selfGuid;
        }

        public ILogReader OpenLog(string path)
        {
            return null;
        }

        public ILogReader OpenLog(string path, LogPosition position, LogPositionOptions position_options)
        {
            return null;
        }
    }
}
