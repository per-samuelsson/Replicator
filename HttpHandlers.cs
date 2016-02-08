using System;
using Starcounter;

namespace LogStreamer {
    class HttpHandlers {
        public HttpHandlers() {
            Handle.GET("/LogStreamer/master", () => {
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

            Handle.GET("/LogStreamer", () => {
                var master = (Master)Self.GET("/LogStreamer/master");

                if (master.CurrentPartial as Home == null) {
                    master.CurrentPartial = new Home()
                    {
                        Data = Program.ParentStatus,
                    };
                    ((Home)master.CurrentPartial).StatusPartial.Data = Program.ParentStatus;
                }

                return master;
            });

            Handle.GET("/LogStreamer/settings", () => {
                var master = (Master)Self.GET("/LogStreamer/master");
                if (master.CurrentPartial as Settings == null) {
                    master.CurrentPartial = new Settings()
                    {
                        Data = Db.SQL<Configuration>("SELECT c FROM LogStreamer.Configuration c WHERE c.DatabaseGuid = ?", Db.Environment.DatabaseGuid.ToString()).First,
                    };
                    ((Settings)master.CurrentPartial).StatusPartial.Data = Program.ParentStatus;
                }

                return master;
            });
        }
    }
}
