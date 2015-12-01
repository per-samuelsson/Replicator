using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public class LogPosition
    {
        public ulong commit_id;
        public int log_file_number_hint;
    }
}
