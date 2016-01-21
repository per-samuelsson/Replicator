
using Starcounter;

namespace Replicator.SweOffshore
{
    public class SweOffshoreSettings
    {
        public SweOffshoreSettings()
        {
            Handle.GET("/Replicator/out/{?}/{?}", (string tableName, string destinationGuid) => { return 200; });
        }
    }
}