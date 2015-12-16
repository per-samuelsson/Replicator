using Starcounter;

namespace Replicator
{
    partial class Status : Partial
    {
        void Handle(Input.Enabled Action)
        {
            Program.ReplicationEnabled = Action.Value;
        }
    }
}
