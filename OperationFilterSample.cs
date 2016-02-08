using System.IO;
using System.Text;
using System.Collections.Generic;
using Starcounter;
using Starcounter.TransactionLog;

namespace LogStreamer
{
    public class OperationFilterSample : IOperationFilter
    {
        private HashSet<string> _negativeCache = new HashSet<string>();

        public bool FilterCreate(string destination, create_record_entry record)
        {
            string baseUri = "/LogStreamer/out/" + record.table + "/";
            bool retv = true; // block sending by default
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
                        // 200 will allow sending unless POST forbids it
                        retv = false;
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
                        retv = false;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeCreateRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = true;
                    }
                }
            }
            return retv;
        }

        public bool FilterUpdate(string destination, update_record_entry record)
        {
            string baseUri = "/LogStreamer/out/" + record.table + "/";
            bool retv = true; // block sending by default
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
                        // 200 will allow sending unless POST forbids it
                        retv = false;
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
                        retv = false;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeUpdateRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = true;
                    }
                }
            }
            return retv;
        }

        public bool FilterDelete(string destination, delete_record_entry record)
        {
            string baseUri = "/LogStreamer/out/" + record.table + "/";
            bool retv = true; // block sending by default
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
                        // 200 will allow sending unless POST forbids it
                        retv = false;
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
                        retv = false;
                        if (response.Body != null)
                        {
                            record = StringSerializer.DeserializeDeleteRecordEntry(new StringReader(response.Body));
                        }
                    }
                    else
                    {
                        // allow POST to override the GET
                        retv = true;
                    }
                }
            }
            return retv;
        }
    }
}
