using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Starcounter.TransactionLog
{
    public struct ReadResult
    {
        public LogPosition continuation_position;
        public TransactionData transaction_data;
    }

    public interface ILogReader
    {
        Task<ReadResult> ReadAsync(CancellationToken ct);
    }
}
