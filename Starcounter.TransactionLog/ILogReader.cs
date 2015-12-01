using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Starcounter.TransactionLog
{
    public class ReadResult
    {
        public LogPosition position;
        public int bytes_read; //more than count if buffer is insufficient
    }

    public interface ILogReader
    {
        Task<ReadResult> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct);
    }
}
