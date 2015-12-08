using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public struct LogPosition
    {
        public ulong address;
        public ulong signature;
        public ulong commit_id;
    }
}
