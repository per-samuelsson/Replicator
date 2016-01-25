using System.IO;
using System.Text;
using System.Collections.Generic;
using Starcounter;
using Starcounter.TransactionLog;

namespace Replicator
{
    public class ReplicationFilterSample : IReplicationFilter
    {
        private HashSet<string> _negativeCache = new HashSet<string>();

        public ReplicationFilterSample() : this(1)
        {
        }

        public ReplicationFilterSample(ulong allowPriority)
        {
            AllowPriority = allowPriority;
        }

        public ulong AllowPriority { get; set; }

        public ulong FilterCreate(string destination, create_record_entry record)
        {
            string baseUri = "/Replicator/out/" + record.table + "/";
            ulong retv = 0; // block replication by default
            Response response;
            lock (_negativeCache)
            {
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.GET(baseUri + destination);
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        // 200 will allow replication unless POST forbids it
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeCreateRecordEntry(new StringReader(response.Body));
                        }
                    }
                }
                baseUri += "create/";
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.POST(baseUri + destination, StringSerializer.Serialize(new StringBuilder(), record).ToString());
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeCreateRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = 0;
                    }
                }
            }
            return retv;
        }

        public ulong FilterUpdate(string destination, update_record_entry record)
        {
            string baseUri = "/Replicator/out/" + record.table + "/";
            ulong retv = 0; // block replication by default
            Response response;
            lock (_negativeCache)
            {
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.GET(baseUri + destination);
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        // 200 will allow replication unless POST forbids it
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeUpdateRecordEntry(new StringReader(response.Body));
                        }
                    }
                }
                baseUri += "update/";
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.POST(baseUri + destination, StringSerializer.Serialize(new StringBuilder(), record).ToString());
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeUpdateRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = 0;
                    }
                }
            }
            return retv;
        }

        public ulong FilterDelete(string destination, delete_record_entry record)
        {
            string baseUri = "/Replicator/out/" + record.table + "/";
            ulong retv = 0; // block replication by default
            Response response;
            lock (_negativeCache)
            {
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.GET(baseUri + destination);
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        // 200 will allow replication unless POST forbids it
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeDeleteRecordEntry(new StringReader(response.Body));
                        }
                    }
                }
                baseUri += "delete/";
                if (!_negativeCache.Contains(baseUri))
                {
                    response = Self.POST(baseUri + destination, StringSerializer.Serialize(new StringBuilder(), record).ToString());
                    if (response == null || response.StatusCode == 404)
                    {
                        _negativeCache.Add(baseUri);
                    }
                    else if (response.StatusCode == 200)
                    {
                        retv = AllowPriority;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeDeleteRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = 0;
                    }
                }
            }
            return retv;
        }
    }
}
