using System;
using Starcounter.TransactionLog;

namespace Replicator
{
    public interface IOperationFilter
    {
        // Filter individual operations in a transaction being considered for replication.
        // Return true to filter out the operation, false to allow replication.
        // The filter may not modify the record entry.
        bool FilterCreate(string destination, create_record_entry cre);
        bool FilterUpdate(string destination, update_record_entry ure);
        bool FilterDelete(string destination, delete_record_entry dre);
    }
}
