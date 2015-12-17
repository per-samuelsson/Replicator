using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Starcounter;

namespace Replicator
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
                    foreach (ReplicationTest foo in Db.SQL<ReplicationTest>("SELECT f FROM Replicator.ReplicationTest f WHERE f.Key = ?", key))
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
                        foreach (ReplicationTest foo in Db.SQL<ReplicationTest>("SELECT f FROM Replicator.ReplicationTest f WHERE f.Key = ?", key))
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
