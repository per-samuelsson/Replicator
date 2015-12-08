using System;
using Starcounter;

namespace Replicator {
    class HttpHandlers {
        public HttpHandlers() {
            Handle.GET("/Replicator/master", () => {
                Session session = Session.Current;

                if (session != null && session.Data != null) {
                    return session.Data;
                }

                var master = new Master();

                if (session == null) {
                    session = new Session(SessionOptions.PatchVersioning);
                }

                master.Session = session;

                return master;
            });

            Handle.GET("/Replicator", () => {
                var master = (Master)Self.GET("/Replicator/master");

                if (master.CurrentPartial as Home == null) {
                    master.CurrentPartial = new Home();
                }

                return master;
            });

            Handle.GET("/Replicator/settings", () => {
                var master = (Master)Self.GET("/Replicator/master");
                if (master.CurrentPartial as Settings == null) {
                    master.CurrentPartial = new Settings()
                    {
                        Data = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", Guid.Empty.ToString()).First,
                    };
                }

                return master;
            });
        }
    }
}
