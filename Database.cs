using Starcounter;

namespace LogStreamer
{
    [Database]
    public class LastPosition
    {
        // The GUID of the database + LogStreamer.TableIdSeparator + table name
        public string TableId;
        // and the last LogPosition we got from them for that table
        public ulong CommitId;
    }
}
