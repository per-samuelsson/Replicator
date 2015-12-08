using Starcounter;
using System;

namespace Replicator {
    partial class Settings : Partial {
        void Handle(Input.SourceIp Action) {
            Console.WriteLine("SourceIp changed: " + Action.Value);
        }

        void Handle(Input.Save Action) {
            Console.WriteLine("Save clicked");
        }
    }
}
