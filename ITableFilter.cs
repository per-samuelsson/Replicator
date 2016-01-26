using System.Collections.Generic;

namespace Replicator
{
    public interface ITableFilter
    {
        // Return null to allow all tables, or a Dictionary<string, ulong> 
        // with the tables to allow and their priorities.
        Dictionary<string, ulong> Factory();
    }
}
