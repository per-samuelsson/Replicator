using Starcounter;

namespace ReplicationTests
{
    [Database]
    public class ReplicationTest
    {
        public int Key;
        public string Value;
    }

    class ReplicationTests
    {
        public ReplicationTests()
        {
            // Allow InvoiceDemo stuff
            Handle.GET("/Replicator/out/Invoice/{?}", (string dbGuid) => { return 200; });
            Handle.GET("/Replicator/out/InvoiceRow/{?}", (string dbGuid) => { return 200; });
            
            // Allow RetailDemo stuff
            Handle.GET("/Replicator/out/ScRetailDemo.ClientStatsEntry/{?}", (string dbGuid) => { return 200; });
            Handle.GET("/Replicator/out/ScRetailDemo.RetailCustomer/{?}", (string dbGuid) => { return 200; });
            Handle.GET("/Replicator/out/ScRetailDemo.Account/{?}", (string dbGuid) => { return 200; });

            // Allow our own test table
            Handle.GET("/Replicator/out/ReplicationTests.ReplicationTest/{?}", (string dbGuid) => { return 200; });

            Handle.GET("/Replicator/test/insert/{?}/{?}", (int key, string value) => {
                Db.Transact(() => {
                    var foo = new ReplicationTest();
                    foo.Key = key;
                    foo.Value = value;
                });
                return 200;
            });

            Handle.GET("/Replicator/test/update/{?}/{?}", (int key, string value) => {
                int count = 0;
                Db.Transact(() =>
                {
                    foreach (ReplicationTest foo in Db.SQL<ReplicationTest>("SELECT f FROM ReplicationTests.ReplicationTest f WHERE f.Key = ?", key))
                    {
                        foo.Value = value;
                        count++;
                    }
                });
                if (count < 1)
                {
                    return 404;
                }
                return 200;
            });

            Handle.GET("/Replicator/test/delete/{?}", (int key) => {
                int count = 0;
                try
                {
                    Db.Transact(() =>
                    {
                        foreach (ReplicationTest foo in Db.SQL<ReplicationTest>("SELECT f FROM ReplicationTests.ReplicationTest f WHERE f.Key = ?", key))
                        {
                            foo.Delete();
                            count++;
                        }
                    });
                }
                catch
                {
                }
                if (count < 1)
                {
                    return 404;
                }
                return 200;
            });

        }
    }
}
