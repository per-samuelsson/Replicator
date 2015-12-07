using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Starcounter.TransactionLog
{
    public interface ILogTransaction
    {
        ulong CommitID();
        Guid DatabaseGuid();
        string Serialize();
        void Deserialize(string data);
        // need something to alter or filter out parts of the transaction,
        // iterating on the classes/tables affected.
    }

    public interface ILogReader
    {
        Task<ILogTransaction> ReadAsync(CancellationToken ct);
    }
}
