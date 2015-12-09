using Starcounter;

namespace Replicator {
    partial class Home : Partial {
        void Handle(Input.ConnectNow Action)
        {
            Program.Connect();
        }
    }
}
