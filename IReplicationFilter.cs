using System;
using Starcounter.TransactionLog;

namespace Replicator
{
    public interface IReplicationFilter
    {
        // Return value is the new replication priority for the table, and will change
        // the priority for all not yet sent operations for that table.
        // Zero will prevent replication.
        // The filter may not modify the record entry.
        ulong FilterCreate(string destination, create_record_entry cre);
        ulong FilterUpdate(string destination, update_record_entry ure);
        ulong FilterDelete(string destination, delete_record_entry dre);
    }
}
