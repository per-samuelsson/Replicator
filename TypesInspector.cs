using System;
using System.Collections.Generic;
using System.Linq;
using Starcounter;

namespace LogStreamer
{
    static class TypesInspector
    {
        public static IEnumerable<Starcounter.Metadata.RawView> GetMissedTables()
        {
            return Db.SQL<Starcounter.Metadata.RawView>("select t from rawview t where updatable = ?", true)
                     .Where(t => Starcounter.Binding.Bindings.GetTypeDef(t.FullName) == null);
        }

        public static IEnumerable<dynamic> GetMissedColumns()
        {
            var all_tables = Db.SQL<Starcounter.Metadata.RawView>("select t from rawview t where updatable = ?", true);

            foreach (var tbl in all_tables)
            {
                var typedef = Starcounter.Binding.Bindings.GetTypeDef(tbl.FullName);
                if (typedef == null)
                    continue;

                for (int i = 2; i < typedef.TableDef.ColumnDefs.Length; ++i)
                {
                    if (!typedef.PropertyDefs.Any(p => p.ColumnIndex == i))
                        yield return new { table = tbl, column = typedef.TableDef.ColumnDefs[i] };
                }
            }
        }

    }
}
