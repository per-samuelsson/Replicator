using Starcounter;

namespace Replicator
{
    partial class Status : Partial
    {
        void Handle(Input.ConnectNow Action)
        {
            Program.Connect();
        }
    }
}
