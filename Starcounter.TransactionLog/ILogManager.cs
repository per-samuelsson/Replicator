using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public interface ILogManager
    {
        ILogReader OpenLog(string path);

        ILogReader OpenLog(string path, LogPosition position);
    }
}
