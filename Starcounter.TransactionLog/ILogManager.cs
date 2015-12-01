using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public enum LogPositionOptions
    {
        ReadFromPosition,
        ReadAfterPosition
    }

    public interface ILogManager
    {
        ILogReader OpenLog(string path);

        ILogReader OpenLog(string path, LogPosition position, LogPositionOptions position_options);
    }
}
