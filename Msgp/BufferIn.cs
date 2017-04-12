using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using Rsqldrv.SqlClient;

// Msqp namespace contains MessagePack encoding and decoding objects.
namespace Rsqldrv.Msgp
{
    // this object reads data from a socket into an internal buffer.
    // Methods are available to decode values from this internal buffer one by one.
    //   The bytes making up a value are copied from internalBuffer into readBuffer, and the decoded value is returned to the caller.
    //   When internalBuffer is empty, it is refilled from the socket.
    class BufferIn
    {
        //===== fields =====

        private static readonly int INTERNAL_BUFFER_DEFAULT_SIZE = 8192;
        private static readonly int READ_BUFFER_DEFAULT_SIZE = 1024;

        private byte[] _internalBuffer = new byte[INTERNAL_BUFFER_DEFAULT_SIZE]; // only used by fetchByte() and PeekByte()
        private int    _available = 0; // number of bytes available in internalBuffer
        private int    _currpos = -1;  // position of next byte to read in internalBuffer
        private Socket _socket;        // internalBuffer is refilled from this socket when it is empty

        private UTF8Encoding _utf8encoder = new UTF8Encoding();

        private byte[] _readBuffer = new byte[READ_BUFFER_DEFAULT_SIZE]; // may reference a new array if a larger one is needed
        private int    _readBufferCount = 0; // number of bytes copied into readBuffer

        //===== constructors =====

        // This constructor links the BufferIn object to the socket passed as argument.
        internal BufferIn(Socket socket)
        {
            this._socket = socket;
        }

        //===== private methods =====

        // returns one byte from internal buffer.
        // If none is available, copy some bytes from the socket into the internal buffer.
        private byte fetchByte()
        {
            if (this._available == 0)
            {
                this._available = this._socket.Receive(this._internalBuffer); // number of bytes read may be smaller than internalBuffer size
                this._currpos = 0;
            }

            byte b = this._internalBuffer[this._currpos];
            this._currpos++;
            this._available--;

            return b;
        }

        // returns one byte from internal buffer.
        // If none is available, copy some bytes from the socket into the internal buffer.
        // The current position in the internal buffer is not changed. 
        private byte peekByte()
        {
            if (this._available == 0)
            {
                this._available = this._socket.Receive(this._internalBuffer); // number of bytes read may be smaller than internalBuffer size
                this._currpos = 0;
            }

            byte b = this._internalBuffer[this._currpos];

            return b;
        }

        // read N bytes into readBuffer.
        private void readNBytes(int n)
        {
            this._readBufferCount = 0;
            if (this._readBuffer.Length < n)
                this._readBuffer = new byte[n+n/4]; // allocate a new buffer a little larger than needed
            
            for (int i = 0; i < n; i++)
                this._readBuffer[i] = this.fetchByte();

            this._readBufferCount = n;
        }

        //===== API methods =====

        // returns the MessagePack type of the current value that we are going to read, by peeking only the first byte.
        // No byte is consumed from the data stream.
        internal MsgpType PeekMsgpType()
        {
            // peek prefix

            byte prefix = this.peekByte();

            // analyze prefix

            if (prefix <= 127) // fixint
                return MsgpType.IntegerType;

            if (prefix >= Prefix.M_NEGATIVE_FIXINT_BASE) // negative fixint
                return MsgpType.IntegerType;

            if ((prefix&Prefix.PREFIX_FIXSTR_MASK) == Prefix.M_FIXSTR_BASE) // fixstr
                return MsgpType.StrType;

            if ((prefix&Prefix.PREFIX_FIXARRAY_MASK) == Prefix.M_FIXARRAY_BASE) // fixarray
                return MsgpType.ArrayType;

            if ((prefix&Prefix.PREFIX_FIXMAP_MASK) == Prefix.M_FIXMAP_BASE) // fixmap
                return MsgpType.MapType;

            switch (prefix)
            {
                case Prefix.M_NIL:
                    return MsgpType.NilType;

                case Prefix.M_FALSE:
                case Prefix.M_TRUE:
                    return MsgpType.BoolType;

                case Prefix.M_UINT8:
                case Prefix.M_UINT16:
                case Prefix.M_UINT32:
                case Prefix.M_UINT64:
                    return MsgpType.UintegerType;

                case Prefix.M_INT8:
                case Prefix.M_INT16:
                case Prefix.M_INT32:
                case Prefix.M_INT64:
                    return MsgpType.IntegerType;

                case Prefix.M_FLOAT64:
                    return MsgpType.Float64Type;

                case Prefix.M_BIN8:
                case Prefix.M_BIN16:
                case Prefix.M_BIN32:
                    return MsgpType.BinType;

                case Prefix.M_STR8:
                case Prefix.M_STR16:
                case Prefix.M_STR32:
                    return MsgpType.StrType;

                case Prefix.M_ARRAY16:
                case Prefix.M_ARRAY32:
                    return MsgpType.ArrayType;

                case Prefix.M_MAP16:
                case Prefix.M_MAP32:
                    return MsgpType.MapType;

                default:
                    throw new DriverException(badPrefixMessage("PeekMsgpType", prefix));
            }
        }

        // reads nil value from the data stream.
        internal void ReadNil()
        {
            byte prefix = this.readPrefix();
            
            if (prefix == Prefix.M_NIL)
                return;

            throw new DriverException(badPrefixMessage("ReadNil", prefix));
        }

        // reads bool value from the data stream.
        internal bool ReadBool()
        {
            byte prefix = this.readPrefix();

            switch (prefix)
            {
                case Prefix.M_FALSE:
                    return false;

                case Prefix.M_TRUE:
                    return true;

                default:
                    throw new DriverException(badPrefixMessage("ReadBool", prefix));
            }
        }

        // reads unsigned 8 bits integer value from the data stream.
        internal byte ReadByte()
        {
            ulong val = this.ReadUlong();

            if (val > Byte.MaxValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a uint8 value.", val));

            return (byte)val;
        }

        // reads unsigned 16 bits integer value from the data stream.
        internal ushort ReadUshort()
        {
            ulong val = this.ReadUlong();

            if (val > UInt16.MaxValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a uint16 value.", val));

            return (ushort)val;
        }

        // reads unsigned 32 bits integer value from the data stream.
        internal uint ReadUint()
        {
            ulong val = this.ReadUlong();

            if (val > UInt32.MaxValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a uint32 value.", val));

            return (uint)val;
        }

        // reads unsigned 64 bits integer value from the data stream.
        internal ulong ReadUlong()
        {
            byte prefix = this.readPrefix();

            if (prefix <= 127) // positive fixint
                return prefix;

            switch (prefix)
            {
                case Prefix.M_UINT8:
                    return this.readRawUINT8();
 
                case Prefix.M_UINT16:
                    return this.readRawUINT16();

                case Prefix.M_UINT32:
                    return this.readRawUINT32();

                case Prefix.M_UINT64:
                    return this.readRawUINT64();

                default:
                    throw new DriverException(badPrefixMessage("ReadUlong", prefix));
            }
        }

        // reads signed 8 bits integer value from the data stream.
        internal sbyte ReadSbyte()
        {
            long val = this.ReadLong();

            if (val > SByte.MaxValue || val < SByte.MinValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a int8 value.", val));

            return (sbyte)val;
        }

        // reads signed 16 bits integer value from the data stream.
        internal short ReadShort()
        {
            long val = this.ReadLong();

            if (val > Int16.MaxValue || val < Int16.MinValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a int16 value.", val));

            return (short)val;
        }

        // reads signed 32 bits integer value from the data stream.
        internal int ReadInt()
        {
            long val = this.ReadLong();

            if (val > Int32.MaxValue || val < Int32.MinValue)
                throw new DriverException(String.Format("MessagePack: received number {0} is not a int32 value.", val));

            return (int)val;
        }

        // reads signed 64 bits integer value from the data stream.
        internal long ReadLong()
        {
            byte prefix = this.readPrefix();

            if (prefix <= 127) // positive fixint
                return prefix;

            if (prefix >= Prefix.M_NEGATIVE_FIXINT_BASE) // negative fixint
                return (long)(sbyte)prefix;

            switch (prefix)
            {
                case Prefix.M_INT8:
		            return this.readRawINT8();

                case Prefix.M_INT16:
		            return this.readRawINT16();

                case Prefix.M_INT32:
		            return this.readRawINT32();

                case Prefix.M_INT64:
                    return this.readRawINT64();

                default:
                    throw new DriverException(badPrefixMessage("ReadLong", prefix));
            }
        }

        // reads a double value from the data stream.
        internal double ReadDouble()
        {
            double val;
            ulong float_bits;

            byte prefix = this.readPrefix();

            if (prefix != Prefix.M_FLOAT64)
                throw new DriverException(badPrefixMessage("ReadDouble", prefix));

            float_bits = this.readRawUINT64();

            val = BitConverter.Int64BitsToDouble((long)float_bits);

            return val;
        }

        // reads a string value from the data stream.
        internal string ReadString()
        {
            int sz = this.readStringHeader();

            this.readNBytes(sz);
            return this._utf8encoder.GetString(this._readBuffer, 0, sz);
        }

        // reads a binary string value from the data stream.
        internal byte[] ReadBytes()
        {
            int sz = this.readBytesHeader();

            this.readNBytes(sz);
            byte[] result = new byte[sz];
            Array.Copy(this._readBuffer, result, sz);

            return result;
        }

        // reads the size of an array from the data stream.
        internal int ReadArrayHeader()
        {
            byte prefix = this.readPrefix();

            if ((prefix&Prefix.PREFIX_FIXARRAY_MASK) == Prefix.M_FIXARRAY_BASE) // fixarray
                return firstBits4(prefix);

            switch (prefix)
            {
                case Prefix.M_ARRAY16:
                    return (int)this.readRawUINT16();

                case Prefix.M_ARRAY32:
                    return (int)this.readRawUINT32();

                default:
                    throw new DriverException(badPrefixMessage("ReadArrayHeader", prefix));
            }
        }

        // reads the size of a map from the data stream.
        internal int ReadMapHeader()
        {
            byte prefix = this.readPrefix();

            if ((prefix&Prefix.PREFIX_FIXMAP_MASK) == Prefix.M_FIXMAP_BASE) // fixmap
                return firstBits4(prefix);

            switch (prefix)
            {
                case Prefix.M_MAP16:
                    return (int)this.readRawUINT16();

                case Prefix.M_MAP32:
                    return (int)this.readRawUINT32();

                default:
                    throw new DriverException(badPrefixMessage("ReadMapHeader", prefix));
            }
        }

        //===== helper methods =====

        private static byte firstBits4(byte b)
        {
            return (byte)(b & 0x0fU);
        }

        private static byte firstBits5(byte b)
        {
            return (byte)(b & 0x1fU);
        }

        // creates an error message for DriverException raised by MessagePack processing.
        private static string badPrefixMessage(string funcname, byte prefix)
        {
            return String.Format("MessagePack {0}: bad prefix byte {1:X2}", funcname, prefix);
        }

        private byte readPrefix()
        {
            this.readNBytes(1);
            return this._readBuffer[0];
        }

        private uint readRawUINT8()
        {
            this.readNBytes(1);
            return this._readBuffer[0];
        }

        private uint readRawUINT16()
        {
            this.readNBytes(2);
            uint val = (uint)this._readBuffer[0]<<8 | (uint)this._readBuffer[1];
            return val;
        }

        private uint readRawUINT32()
        {
            this.readNBytes(4);
            uint val = (uint)this._readBuffer[0]<<24 | (uint)this._readBuffer[1]<<16 | (uint)this._readBuffer[2]<<8 | (uint)this._readBuffer[3];
            return val;
        }

        private ulong readRawUINT64()
        {
            this.readNBytes(8);
            ulong val = (ulong)this._readBuffer[0]<<56 | (ulong)this._readBuffer[1]<<48 | (ulong)this._readBuffer[2]<<40 | (ulong)this._readBuffer[3]<<32 | (ulong)this._readBuffer[4]<<24 | (ulong)this._readBuffer[5]<<16 | (ulong)this._readBuffer[6]<<8 | (ulong)this._readBuffer[7];
            return val;
        }

        private int readRawINT8()
        {
            return (int)this.readRawUINT8();
        }

        private int readRawINT16()
        {
            return (int)this.readRawUINT16();
        }

        private int readRawINT32()
        {
            return (int)this.readRawUINT32();
        }

        private long readRawINT64()
        {
            return (long)this.readRawUINT64();
        }

        // reads the size of a string value from the data stream.
        private int readStringHeader()
        {
            byte prefix = this.readPrefix();

            if ((prefix & Prefix.PREFIX_FIXSTR_MASK) == Prefix.M_FIXSTR_BASE) // fixstr
                return firstBits5(prefix);

            switch (prefix)
            {
                case Prefix.M_STR8:
                    return (int)this.readRawUINT8();

                case Prefix.M_STR16:
                    return (int)this.readRawUINT16();

                case Prefix.M_STR32:
                    return (int)this.readRawUINT32();

                default:
                    throw new DriverException(badPrefixMessage("ReadStringHeader", prefix));
            }
        }

        // reads the size of a binary string value from the data stream.
        private int readBytesHeader()
        {
            byte prefix = this.readPrefix();

            switch (prefix)
            {
                case Prefix.M_BIN8:
                    return (int)this.readRawUINT8();

                case Prefix.M_BIN16:
                    return (int)this.readRawUINT16();

                case Prefix.M_BIN32:
                    return (int)this.readRawUINT32();

                default:
                    throw new DriverException(badPrefixMessage("ReadBytesHeader", prefix));
            }
        }

    }
}
