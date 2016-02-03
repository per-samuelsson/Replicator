using System;
using Starcounter;

namespace Replicator {
    class HttpHandlers {
        public HttpHandlers() {
            Handle.GET("/Replicator/master", () => {
                Session session = Session.Current;
                if (session != null)
                {
                    lock (Program.MySessions)
                    {
                        Program.MySessions.Add(session.ToAsciiString());
                    }
                    if (session.Data != null)
                    {
                        return session.Data;
                    }
                }

                Master master = null;

                Db.Scope(() =>
                {
                    master = new Master();
                });

                if (session == null)
                {
                    session = new Session(SessionOptions.PatchVersioning);
                    lock (Program.MySessions)
                    {
                        Program.MySessions.Add(session.ToAsciiString());
                    }
                }

                master.Session = session;

                return master;
            });

            Handle.GET("/Replicator", () => {
                var master = (Master)Self.GET("/Replicator/master");

                if (master.CurrentPartial as Home == null) {
                    master.CurrentPartial = new Home()
                    {
                        Data = Program.ParentStatus,
                    };
                    ((Home)master.CurrentPartial).StatusPartial.Data = Program.ParentStatus;
                }

                return master;
            });

            Handle.GET("/Replicator/settings", () => {
                var master = (Master)Self.GET("/Replicator/master");
                if (master.CurrentPartial as Settings == null) {
                    master.CurrentPartial = new Settings()
                    {
                        Data = Db.SQL<Configuration>("SELECT c FROM Replicator.Configuration c WHERE c.DatabaseGuid = ?", Db.Environment.DatabaseGuid.ToString()).First,
                    };
                    ((Settings)master.CurrentPartial).StatusPartial.Data = Program.ParentStatus;
                }

                return master;
            });
        }
    }
}
