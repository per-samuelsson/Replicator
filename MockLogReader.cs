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
        private LogPosition _position;

        public MockLogReader(Guid dbGuid, LogPosition position)
        {
            _dbGuid = dbGuid;
            _position = position;
        }

        public Task<LogReadResult> ReadAsync(CancellationToken ct)
        {
            var t = new Task<LogReadResult>(() =>
            {
                Task.Delay(1000).Wait();
                _position.commit_id++;

                var creates = new List<create_record_entry>();
                var create_columns = new column_update[1];
                create_columns[0].name = "BarColumn";
                create_columns[0].value = DateTime.Now.ToString();
                creates.Add(new create_record_entry() {
                    table = "FooTable",
                    columns = create_columns,
                });

                var updates = new List<update_record_entry>();
                var update_columns = new column_update[1];
                update_columns[0].name = "DatabaseGuid";
                update_columns[0].value = _dbGuid.ToString(); // This should not get filtered
                updates.Add(new update_record_entry()
                {
                    table = "Replicator.Replication",
                    columns = update_columns,
                });

                return new LogReadResult() {
                    continuation_position = _position,
                    transaction_data = new TransactionData()
                    {
                        creates = creates,
                        updates = updates,
                        deletes = new List<delete_record_entry>(),
                    }
                };
            }, ct);
            t.Start();
            return t;
        }
    }
}
