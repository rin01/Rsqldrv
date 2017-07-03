using System;
using System.Text;
using System.Collections.Generic;
using Rsqldrv;

// Msqp namespace contains MessagePack encoding and decoding objects.
namespace Rsqldrv.Msgp
{
    // this object appends values encoded in MessagePack encoding in an internal byte buffer.
    // The content of this byte buffer can than be sent into a socket.
    class BufferOut
    {
        //===== fields =====

        private static readonly int BUFF_INITIAL_CAPACITY = 256;
        private List<byte> _buff = new List<byte>(BUFF_INITIAL_CAPACITY); // internal byte buffer

        private UTF8Encoding _utf8encoder = new UTF8Encoding(); // no BOM, and doesn't throw exception for invalid byte sequence

        //===== API methods =====

        // resets the byte buffer to a length of 0.
        internal void Reset() {
            this._buff.Clear();
        }

        // returns the number of bytes used by the string, encoded in utf-8.
        internal int GetByteCount(string s)
        {
            int sz = this._utf8encoder.GetByteCount(s);

            return sz;
        }

        // copies the content of the buffer in a new byte array and returns it.
        internal byte[] ToByteArray()
        {
            return this._buff.ToArray();
        }

        internal void AppendNil()
        {
            this._buff.Add(Prefix.M_NIL);
        }

        internal void AppendBool(bool val)
        {
            byte b = Prefix.M_FALSE; // false
            if (val) {
                b = Prefix.M_TRUE; // true
            }

            this._buff.Add(b);
        }

        internal void AppendUlong(ulong val)
        {
            if (val <= 127)
            {
               this._buff.Add((byte)val); // positive fixint
            }
            else if (val <= Byte.MaxValue)
            {
                this._buff.AddMany(Prefix.M_UINT8, (byte)val);
            }
            else if (val <= UInt16.MaxValue)
            {
                this._buff.AddMany(Prefix.M_UINT16, (byte)(val>>8), (byte)val);
            }
            else if (val <= UInt32.MaxValue)
            {
                this._buff.AddMany(Prefix.M_UINT32, (byte)(val>>24), (byte)(val>>16), (byte)(val>>8), (byte)val);
            }
            else {
                this._buff.AddMany(Prefix.M_UINT64, (byte)(val>>56), (byte)(val>>48), (byte)(val>>40), (byte)(val>>32), (byte)(val>>24), (byte)(val>>16), (byte)(val>>8), (byte)val);
            }
        }

        internal void AppendLong(long val)
        {
            if (val >= 0)
            {
                if (val <= 127)
                {
                    this._buff.Add((byte)val); // positive fixint
                }
                // else if (val <= Sbyte.MaxValue)  // not used, as it matches    if (val <= 127)
                // {
                //       this._buff.AddMany(Prefix.M_INT8, (byte)val);
                // }
                else if (val <= Int16.MaxValue)
                {
                    this._buff.AddMany(Prefix.M_INT16, (byte)(val>>8), (byte)val);
                }
                else if (val <= Int32.MaxValue)
                {
                    this._buff.AddMany(Prefix.M_INT32, (byte)(val>>24), (byte)(val>>16), (byte)(val>>8), (byte)val);
                }
                else
                {
                    this._buff.AddMany(Prefix.M_INT64, (byte)(val>>56), (byte)(val>>48), (byte)(val>>40), (byte)(val>>32), (byte)(val>>24), (byte)(val>>16), (byte)(val>>8), (byte)val);
                }

                return;
            }

            // negative number

            if (val >= -32) // 0xe0  11100000
            {
                this._buff.Add((byte)val); // negative fixint
            }
            else if (val >= SByte.MinValue)
            {
                this._buff.AddMany(Prefix.M_INT8, (byte)val);
            }
            else if (val >= Int16.MinValue)
            {
                this._buff.AddMany(Prefix.M_INT16, (byte)(val>>8), (byte)val);
            }
            else if (val >= Int32.MinValue)
            {
                this._buff.AddMany(Prefix.M_INT32, (byte)(val>>24), (byte)(val>>16), (byte)(val>>8), (byte)val);
            }
            else
            {
                this._buff.AddMany(Prefix.M_INT64, (byte)(val >> 56), (byte)(val >> 48), (byte)(val >> 40), (byte)(val >> 32), (byte)(val >> 24), (byte)(val >> 16), (byte)(val >> 8), (byte)val);
            }
        }

        internal void AppendString(string s)
        {
            byte[] bytes = this._utf8encoder.GetBytes(s);
            int sz = bytes.Length;

            this.appendStringHeader(sz);

            foreach (var b in bytes)
            {
                this._buff.Add(b);
            }
        }

        internal void AppendBytes(byte[] bytes)
        {
            int sz = bytes.Length;

            this.appendBytesHeader(sz);

            foreach (var b in bytes)
            {
                this._buff.Add(b);
            }
        }

        //========= more complex types =========

        internal void AppendSimpleType(object x)
        {

            if (x == null) {
                this.AppendNil();
                return;
            }

            if (x is byte[]) {
                this.AppendBytes((byte[])x);
                return;
            }

            TypeCode typeCode = Type.GetTypeCode(x.GetType());

            switch (typeCode)
            {
                case TypeCode.String:
                    this.AppendString((string)x);
                    return;

                case TypeCode.Boolean:
                    this.AppendBool((bool)x);
                    return;

                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    this.AppendUlong((ulong)x);
                    return;

                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                    this.AppendLong((long)x);
                    return;

                default:
                    throw new RsqlAssertFailedException("msgp: AppendSimpleType: type not supported");
            }
        }

        internal void AppendMapStrStr(Dictionary<string, string> argMap)
        {
        	int sz = argMap.Count;
            this.appendMapHeader(sz);

            foreach (KeyValuePair<string, string> entry in argMap)
            {
                this.AppendString(entry.Key);
                this.AppendString(entry.Value);
            }
        }

        internal void AppendMapStrSimpleType(Dictionary<string, object> argMap)
        {
            int sz = argMap.Count;
            this.appendMapHeader(sz);

            foreach (KeyValuePair<string, object> entry in argMap)
            {
                this.AppendString(entry.Key);
                this.AppendSimpleType(entry.Value);
            }
        }

        //===== helper methods =====

        private void appendStringHeader(int size)
        {
            uint sz = (uint)size; // we will never have data larger than 2 Gb, so the cast is ok

            if (sz <= 31) // 0x1f  00011111
            {
                this._buff.Add((byte)(Prefix.M_FIXSTR_BASE | (byte)sz)); // fixstr
            }
            else if (sz <= Byte.MaxValue)
            {
                this._buff.Add(Prefix.M_STR8);
                this._buff.Add((byte)sz);
            }
            else if (sz <= UInt16.MaxValue)
            {
                this._buff.Add(Prefix.M_STR16);
                this._buff.AddMany((byte)(sz >> 8), (byte)sz);
            }
            else
            {
                this._buff.Add(Prefix.M_STR32);
                this._buff.AddMany((byte)(sz >> 24), (byte)(sz >> 16), (byte)(sz >> 8), (byte)sz);
            }
        }

        private void appendBytesHeader(int size)
        {
            uint sz = (uint)size; // we will never have data larger than 2 Gb, so the cast is ok

            if (sz <= Byte.MaxValue)
            {
                this._buff.Add(Prefix.M_BIN8);
                this._buff.Add((byte)sz);
            }
            else if (sz <= UInt16.MaxValue)
            {
                this._buff.Add(Prefix.M_BIN16);
                this._buff.AddMany((byte)(sz >> 8), (byte)sz);
            }
            else
            {
                this._buff.Add(Prefix.M_BIN32);
                this._buff.AddMany((byte)(sz >> 24), (byte)(sz >> 16), (byte)(sz >> 8), (byte)sz);
            }
        }

        private void appendArrayHeader(int size)
        {
            uint sz = (uint)size; // we will never have more than 2 billions elements in the array, so the cast is ok

            if (sz <= 15) // 0x0f    00001111
            {
                this._buff.Add((byte)(Prefix.M_FIXARRAY_BASE | (byte)sz));
            }
            else if (sz <= UInt16.MaxValue)
            {
                this._buff.Add(Prefix.M_ARRAY16);
                this._buff.AddMany((byte)(sz >> 8), (byte)sz);
            }
            else
            {
                this._buff.Add(Prefix.M_ARRAY32);
                this._buff.AddMany((byte)(sz >> 24), (byte)(sz >> 16), (byte)(sz >> 8), (byte)sz);
            }
        }

        private void appendMapHeader(int size)
        {
            uint sz = (uint)size; // we will never have more than 2 billions elements in the map, so the cast is ok

            if (sz <= 15) // 0x0f    00001111
            {
                this._buff.Add((byte)(Prefix.M_FIXMAP_BASE | (byte)sz));
            }
            else if (sz <= UInt16.MaxValue)
            {
                this._buff.Add(Prefix.M_MAP16);
                this._buff.AddMany((byte)(sz >> 8), (byte)sz);
            }
            else
            {
                this._buff.Add(Prefix.M_MAP32);
                this._buff.AddMany((byte)(sz >> 24), (byte)(sz >> 16), (byte)(sz >> 8), (byte)sz);
            }
        }

    }
}
