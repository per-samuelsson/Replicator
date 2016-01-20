using Starcounter;

namespace Replicator {

    partial class Home : Partial {

        protected override void OnData()
        {
            base.OnData();
            this.StatusPartial.Enabled = Program.ReplicationEnabled;
        }
    }
}