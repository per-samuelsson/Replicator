using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    interface ILogApplicator
    {
        void Apply(TransactionData transaction_data);
    }
}
