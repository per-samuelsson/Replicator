
using Starcounter;
using Starcounter.Metadata;
using System;
using System.Collections.Generic;

namespace Replicator.SweOffshore
{
    public class SweOffshoreSettings
    {
        public static void BuildTablePriorities(Dictionary<string, int> priorities)
        {
            var nameComparison = StringComparison.InvariantCultureIgnoreCase;

            foreach (var c in Db.SQL<ClrClass>("SELECT c FROM Starcounter.Metadata.ClrClass c"))
            {
                if (c.FullClassName.StartsWith("Sweoffshore", nameComparison))
                {
                    priorities.Add(c.FullClassName, 1);
                }
                else if (c.FullClassName.StartsWith("Simplified", nameComparison))
                {
                    priorities.Add(c.FullClassName, 1);
                }
            }
        }
    }
}