using System;
using System.IO;
using System.Text;
using Starcounter.TransactionLog;
using System.Collections.Generic;

namespace LogStreamer
{
    public class StringSerializer
    {
        private const char TypeReference = 'r';
        private const char TypeString = '"';
        private const char TypeLong = 'l';
        private const char TypeULong = 'u';
        private const char TypeDecimal = 'e';
        private const char TypeFloat = 'f';
        private const char TypeDouble = 'd';
        private const char TypeByteArray = 'b';
        private const char TypeLogReadResult = 'R';
        private const char TypeLogPosition = 'P';
        private const char TypeTransactionData = 'T';
        private const char TypeCreateRecordEntry = 'I';
        private const char TypeUpdateRecordEntry = 'U';
        private const char TypeDeleteRecordEntry = 'D';
        private const char TypeColumnUpdate = 'C';

        private static readonly uint[] _lookup32 = CreateLookup32();

        private static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s = i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }

        private static string ByteArrayToHexViaLookup32(byte[] bytes)
        {
            var lookup32 = _lookup32;
            var result = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }

        private static int GetHexVal(int hex)
        {
            return (hex - (hex < 58 ? 48 : 55)) & 0xF;
        }

        static public StringBuilder Serialize(StringBuilder sb, reference r)
        {
            sb.Append(TypeReference);
            sb.Append(r.object_id);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, string s)
        {
            sb.Append('"');
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (ch == '"' || ch == '\\')
                {
                    sb.Append('\\');
                }
                sb.Append(ch);
            }
            sb.Append('"');
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, long l)
        {
            sb.Append(TypeLong);
            sb.Append(l);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, ulong ul)
        {
            sb.Append(TypeULong);
            sb.Append(ul);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, decimal d)
        {
            sb.Append(TypeDecimal);
            sb.Append(d);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, float f)
        {
            sb.Append(TypeFloat);
            sb.Append(f);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, double d)
        {
            sb.Append(TypeDouble);
            sb.Append(d);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, byte[] b)
        {
            sb.Append(TypeByteArray);
            sb.Append(ByteArrayToHexViaLookup32(b));
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, LogReadResult lrr)
        {
            sb.Append(TypeLogReadResult);
            Serialize(sb, lrr.continuation_position);
            Serialize(sb, lrr.transaction_data);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, LogPosition lp)
        {
            sb.Append(TypeLogPosition);
            Serialize(sb, lp.commit_id);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, TransactionData tran)
        {
            sb.Append(TypeTransactionData);
            for (int index = 0; index < tran.creates.Count; index++)
                Serialize(sb, tran.creates[index]);
            for (int index = 0; index < tran.updates.Count; index++)
                Serialize(sb, tran.updates[index]);
            for (int index = 0; index < tran.deletes.Count; index++)
                Serialize(sb, tran.deletes[index]);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, create_record_entry cre)
        {
            sb.Append(TypeCreateRecordEntry);
            Serialize(sb, cre.table);
            Serialize(sb, cre.key);
            for (int index = 0; index < cre.columns.Length; index++)
                Serialize(sb, cre.columns[index]);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, update_record_entry ure)
        {
            sb.Append(TypeUpdateRecordEntry);
            Serialize(sb, ure.table);
            Serialize(sb, ure.key);
            for (int index = 0; index < ure.columns.Length; index++)
                Serialize(sb, ure.columns[index]);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, delete_record_entry dre)
        {
            sb.Append(TypeDeleteRecordEntry);
            Serialize(sb, dre.table);
            Serialize(sb, dre.key);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder Serialize(StringBuilder sb, column_update cu)
        {
            sb.Append(TypeColumnUpdate);
            Serialize(sb, cu.name);
            SerializeValue(sb, cu.value);
            sb.Append(' ');
            return sb;
        }

        static public StringBuilder SerializeValue(StringBuilder sb, object o)
        {
            if (o is ulong)
            {
                return Serialize(sb, (ulong)o);
            }

            if (o is long)
            {
                return Serialize(sb, (long)o);
            }

            if (o is reference)
            {
                return Serialize(sb, (reference)o);
            }

            if (o is string)
            {
                return Serialize(sb, (string)o);
            }

            if (o is decimal)
            {
                return Serialize(sb, (decimal)o);
            }

            if (o is float)
            {
                return Serialize(sb, (float)o);
            }

            if (o is double)
            {
                return Serialize(sb, (double)o);
            }

            if (o is byte[])
            {
                return Serialize(sb, (byte[])o);
            }

            throw new ArgumentException("unhandled data type " + o.GetType().ToString());
        }

        static private void CheckInitialChar(StringReader sr, char ch)
        {
            SkipWS(sr);
            if ((char)sr.Read() != ch)
            {
                throw new InvalidDataException("expected " + ch);
            }
        }

        static public int SkipWS(StringReader sr)
        {
            while (Char.IsWhiteSpace((char)sr.Peek()))
                sr.Read();
            return sr.Peek();
        }

        static public ulong DeserializeDigits(StringReader sr)
        {
            ulong accum = 0;
            while (Char.IsDigit((char)sr.Peek()))
            {
                accum *= 10;
                accum += (ulong)((char)sr.Read() - '0');
            }
            return accum;
        }

        static public string DeserializeNonWS(StringReader sr)
        {
            StringBuilder sb = new StringBuilder();
            int ch = sr.Read();
            while (ch != -1 && !Char.IsWhiteSpace((char)ch))
            {
                sb.Append((char)ch);
                ch = sr.Read();
            }
            return sb.ToString();
        }

        static private StringBuilder _sb = new StringBuilder();

        static public string DeserializeString(StringReader sr)
        {
            CheckInitialChar(sr, '"');
            _sb.Clear();
            for (int ch = sr.Read(); ch != -1; ch = sr.Read())
            {
                if ((char)ch == '"')
                    return _sb.ToString();
                if ((char)ch == '\\')
                {
                    ch = sr.Read();
                    if (ch == -1)
                        break;
                }
                _sb.Append((char)ch);
            }
            throw new InvalidDataException("unterminated string");
        }

        static public reference DeserializeReference(StringReader sr)
        {
            CheckInitialChar(sr, TypeReference);
            return new reference { object_id = DeserializeDigits(sr) };
        }

        static public ulong DeserializeULong(StringReader sr)
        {
            CheckInitialChar(sr, TypeULong);
            return DeserializeDigits(sr);
        }

        static public long DeserializeLong(StringReader sr)
        {
            CheckInitialChar(sr, TypeLong);
            if ((char)sr.Peek() == '-')
            {
                sr.Read();
                return -(long)DeserializeDigits(sr);
            }
            return (long)DeserializeDigits(sr);
        }

        static public decimal DeserializeDecimal(StringReader sr)
        {
            CheckInitialChar(sr, TypeDecimal);
            return Convert.ToDecimal(DeserializeNonWS(sr));
        }

        static public float DeserializeFloat(StringReader sr)
        {
            CheckInitialChar(sr, TypeFloat);
            return (float)DeserializeDouble(sr);
        }

        static public double DeserializeDouble(StringReader sr)
        {
            CheckInitialChar(sr, TypeDouble);
            return Convert.ToDouble(DeserializeNonWS(sr));
        }

        static public byte[] DeserializeByteArray(StringReader sr)
        {
            CheckInitialChar(sr, TypeByteArray);
            MemoryStream ms = new MemoryStream();
            int ch = sr.Read();
            while (ch != -1 && !Char.IsWhiteSpace((char)ch))
            {
                ms.WriteByte((byte)((GetHexVal(ch) << 4) | GetHexVal(sr.Read())));
            }
            return ms.ToArray();
        }

        static public LogReadResult DeserializeLogReadResult(StringReader sr)
        {
            CheckInitialChar(sr, TypeLogReadResult);
            return new LogReadResult
            {
                continuation_position = DeserializeLogPosition(sr),
                transaction_data = DeserializeTransactionData(sr)
            };
        }

        static public LogPosition DeserializeLogPosition(StringReader sr)
        {
            CheckInitialChar(sr, TypeLogPosition);
            return new LogPosition
            {
                commit_id = DeserializeULong(sr)
            };
        }

        static public TransactionData DeserializeTransactionData(StringReader sr)
        {
            CheckInitialChar(sr, TypeTransactionData);
            var creates = new List<create_record_entry>();
            var updates = new List<update_record_entry>();
            var deletes = new List<delete_record_entry>();

            for (int ch = SkipWS(sr); ch != -1; ch = SkipWS(sr))
            {
                if ((char)ch == TypeCreateRecordEntry)
                {
                    creates.Add(DeserializeCreateRecordEntry(sr));
                    continue;
                }

                if ((char)ch == TypeUpdateRecordEntry)
                {
                    updates.Add(DeserializeUpdateRecordEntry(sr));
                    continue;
                }

                if ((char)ch == TypeDeleteRecordEntry)
                {
                    deletes.Add(DeserializeDeleteRecordEntry(sr));
                    continue;
                }

                break;
            }

            return new TransactionData
            {
                creates = creates,
                updates = updates,
                deletes = deletes
            };
        }

        static public column_update[] DeserializeColumnUpdateArray(StringReader sr)
        {
            var retv = new List<column_update>();

            for (int ch = SkipWS(sr); ch != -1; ch = SkipWS(sr))
            {
                if (ch != TypeColumnUpdate)
                    break;
                retv.Add(DeserializeColumnUpdate(sr));
            }

            return retv.ToArray();
        }

        static public column_update DeserializeColumnUpdate(StringReader sr)
        {
            CheckInitialChar(sr, TypeColumnUpdate);
            return new column_update
            {
                name = DeserializeString(sr),
                value = DeserializeValue(sr)
            };
        }

        static public create_record_entry DeserializeCreateRecordEntry(StringReader sr)
        {
            CheckInitialChar(sr, TypeCreateRecordEntry);
            return new create_record_entry
            {
                table = DeserializeString(sr),
                key = DeserializeReference(sr),
                columns = DeserializeColumnUpdateArray(sr)
            };
        }

        static public update_record_entry DeserializeUpdateRecordEntry(StringReader sr)
        {
            CheckInitialChar(sr, TypeUpdateRecordEntry);
            return new update_record_entry
            {
                table = DeserializeString(sr),
                key = DeserializeReference(sr),
                columns = DeserializeColumnUpdateArray(sr)
            };
        }

        static public delete_record_entry DeserializeDeleteRecordEntry(StringReader sr)
        {
            CheckInitialChar(sr, TypeDeleteRecordEntry);
            return new delete_record_entry
            {
                table = DeserializeString(sr),
                key = DeserializeReference(sr)
            };
        }

        static public object DeserializeValue(StringReader sr)
        {
            int nextch = SkipWS(sr);
            if (nextch != -1)
            {
                char ch = (char)nextch;

                if (ch == TypeReference)
                {
                    return DeserializeReference(sr);
                }

                if (ch == TypeString)
                {
                    return DeserializeString(sr);
                }

                if (ch == TypeLong)
                {
                    return DeserializeLong(sr);
                }

                if (ch == TypeULong)
                {
                    return DeserializeULong(sr);
                }

                if (ch == TypeDecimal)
                {
                    return DeserializeDecimal(sr);
                }

                if (ch == TypeFloat)
                {
                    return DeserializeFloat(sr);
                }

                if (ch == TypeDouble)
                {
                    return DeserializeDouble(sr);
                }

                if (ch == TypeByteArray)
                {
                    return DeserializeByteArray(sr);
                }
            }

            throw new InvalidDataException("expected value");
        }
    }
}
