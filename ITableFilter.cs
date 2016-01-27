using System.Collections.Generic;

namespace Replicator
{
    public interface ITableFilter
    {
        // Return null to allow all tables, or a Dictionary<string, int> 
        // with the tables to allow and their priorities. Tables with a
        // a higher priority will be sent before tables with a lower 
        // priority value.
        Dictionary<string, int> Factory();
    }
}
