using Starcounter;

namespace LogStreamer
{
    partial class Status : Partial
    {
        void Handle(Input.Enabled Action)
        {
            Program.StreamingEnabled = Action.Value;
        }
    }
}
