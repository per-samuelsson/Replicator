using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Starcounter.TransactionLog;

namespace Replicator
{
    class MockLogReader : ILogReader
    {
        private Guid _dbGuid;
        private ulong _commitId;

        public MockLogReader(Guid dbGuid, ulong commitId)
        {
            _dbGuid = dbGuid;
            _commitId = commitId;
        }

        public Task<ILogTransaction> ReadAsync(CancellationToken ct)
        {
            var t = new Task<ILogTransaction>(() =>
            {
                Task.Delay(1000).Wait();
                _commitId++;
                return new MockLogTransaction(_dbGuid, _commitId);
            }, ct);
            t.Start();
            return t;
        }
    }
}
