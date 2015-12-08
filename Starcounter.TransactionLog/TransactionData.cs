using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public struct reference
    {
        public ulong object_id;
    }

    public struct column_update
    {
        public string name;
        public object value; //reference, sring, long, ulong, decimal, float, double, byte[]
    };

    public struct create_record_entry
    {
        public string table;
        public reference key;
        public column_update[] columns;
    };

    public struct update_record_entry
    {
        public string table;
        public reference key;
        public column_update[] columns;
    };

    public struct delete_record_entry
    {
        public string table;
        public reference key;
    };

    public class TransactionData
    {
        public List<create_record_entry> creates;
        public List<update_record_entry> updates;
        public List<delete_record_entry> deletes;
    }
}
