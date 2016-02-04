using Starcounter;

namespace LogStreamer {

    partial class Home : Partial {

        protected override void OnData()
        {
            base.OnData();
            this.StatusPartial.Enabled = Program.StreamingEnabled;
        }
    }
}