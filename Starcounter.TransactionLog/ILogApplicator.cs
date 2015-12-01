using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Starcounter.TransactionLog
{
    public interface ILogApplicator
    {
        void Apply(byte[] payload_buffer, int offset, int count);
    }
}
