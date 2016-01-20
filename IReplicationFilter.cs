using System;
using Starcounter.TransactionLog;

namespace Replicator
{
    public interface IReplicationFilter
    {
        // Return value is replication priority. Zero will prevent replication.
        // The filter may modify the record entry.
        ulong FilterCreate(string destination, ref create_record_entry cre);
        ulong FilterUpdate(string destination, ref update_record_entry ure);
        ulong FilterDelete(string destination, ref delete_record_entry dre);
    }
}
