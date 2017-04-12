using System;
using System.Data;
using System.Data.SqlTypes;
using Rsqldrv.Msgp;

namespace Rsqldrv.SqlClient
{
    // a row is made up of objects like FieldInt, FieldDatetime, etc. They all implement IField interface.
    internal interface IField
    {
        object GetValue(); // if NULL, returns System.DBNull.Value
        object GetSqlValue();
        SqlDbType Datatype();
        bool IsNULL();
        void ReadValue(BufferIn buff);
    }

    internal class FieldVoid : IField
    {
        private SqlBoolean _val; // always contains the NULL value

        internal FieldVoid()
        {
            this._val = SqlBoolean.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Variant;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            buff.ReadNil(); // it must always be Prefix.M_NIL
        }

        public object GetValue()
        {
            return System.DBNull.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldBoolean : IField
    {
        private SqlBoolean _val;

        internal FieldBoolean()
        {
            this._val = SqlBoolean.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Bit; // there is no SqlDbType.Boolean, but Bit is ok
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlBoolean.Null;
                return;
            }

            // value

            bool valbool = buff.ReadBool();
            this._val = new SqlBoolean(valbool);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldVarbinary : IField
    {
        private SqlBinary _val;
        private uint _precision;

        internal FieldVarbinary(uint precision)
        {
            this._val = SqlBinary.Null;
            this._precision = precision;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.VarBinary;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlBinary.Null;
                return;
            }

            // value

            byte[] valbytes = buff.ReadBytes();
            this._val = new SqlBinary(valbytes);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value; // it is ok to return a reference to the internal byte array used by SqlBinary
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldVarchar : IField
    {
        private SqlString _val;
        private uint _precision;
        private bool _fixlen;

        internal FieldVarchar(uint precision, bool fixlen)
        {
            this._val = SqlString.Null;
            this._precision = precision;
            this._fixlen = fixlen;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.VarChar;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlString.Null;
                return;
            }

            // value

            string valstr = buff.ReadString();
            if (this._fixlen == true && valstr.Length < this._precision) // pad for CHAR
                valstr = valstr.PadRight((int)this._precision);

            this._val = new SqlString(valstr);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldBit : IField
    {
        private SqlBoolean _val;

        internal FieldBit()
        {
            this._val = SqlBoolean.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Bit;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlBoolean.Null;
                return;
            }

            // value

            byte val8 = buff.ReadByte();
            this._val = new SqlBoolean(val8);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldTinyint : IField
    {
        private SqlByte _val;

        internal FieldTinyint()
        {
            this._val = SqlByte.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.TinyInt;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlByte.Null;
                return;
            }

            // value

            byte val8 = buff.ReadByte();
            this._val = new SqlByte(val8);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldSmallint : IField
    {
        private SqlInt16 _val;

        internal FieldSmallint()
        {
            this._val = SqlInt16.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.SmallInt;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlInt16.Null;
                return;
            }

            // value

            short val16 = buff.ReadShort();
            this._val = new SqlInt16(val16);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldInt : IField
    {
        private SqlInt32 _val;

        internal FieldInt()
        {
            this._val = SqlInt32.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Int;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlInt32.Null;
                return;
            }

            // value

            int val32 = buff.ReadInt();
            this._val = new SqlInt32(val32);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldBigint : IField
    {
        private SqlInt64 _val;

        internal FieldBigint()
        {
            this._val = SqlInt64.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.BigInt;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlInt64.Null;
                return;
            }

            // value

            long val64 = buff.ReadLong();
            this._val = new SqlInt64(val64);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldMoney : IField
    {
        private SqlMoney _val;
        private uint _precision;
        private uint _scale;

        internal FieldMoney(uint precision, uint scale)
        {
            this._val = SqlMoney.Null;
            this._precision = precision;
            this._scale = scale;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Money;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlMoney.Null;
                return;
            }

            // value

            string valstr = buff.ReadString();
            this._val = SqlMoney.Parse(valstr);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldNumeric : IField
    {
        private SqlDecimal _val;
        private uint _precision;
        private uint _scale;

        internal FieldNumeric(uint precision, uint scale)
        {
            this._val = SqlDecimal.Null;
            this._precision = precision;
            this._scale = scale;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Decimal;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlDecimal.Null;
                return;
            }

            // value

            string valstr = buff.ReadString();
            this._val = SqlDecimal.Parse(valstr);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            const int DOTNET_DECIMAL_DATATYPE_PRECISION = 28; // .Net decimal has precision of 28, whereas SqlDecimal has precision of 38

            // .Net decimal has precision of 28, whereas SqlDecimal has precision of 38.
            // if precision o SqlDecimal value > 28, it cannot be converted to decimal.
            // Even a SqlDecimal of 1.00000000000000000000000000000 cannot be converted, as non-significant trailing 0 are not dicarded !
            // MS SQL Server driver throws an OverflowException.
            // But  a SqlDecimal of 1.0000000000000000000000000000 will be converted without problem.
            // MS doesn't provide a function to rescale by removing non-significant trailing 0s.
            // The behaviour of the SqlDecimal.Value is stupid !

            if (this._val.Precision > DOTNET_DECIMAL_DATATYPE_PRECISION) // 28 is the precision of .Net decimal type
            {
                int newscale = (int)this._val.Scale - ((int)this._val.Precision - DOTNET_DECIMAL_DATATYPE_PRECISION) -1; // -1 because rounding occurs, e.g. 9.999 -> 10.0
                int newprec = DOTNET_DECIMAL_DATATYPE_PRECISION;

                if (newscale >= 0)
                {
                    SqlDecimal decval = SqlDecimal.ConvertToPrecScale(this._val, newprec, newscale); // MS driver doesn't do this rescaling, and will throw an exception
                    return decval.Value; // without the rescaling on the line above, this line would throw an OverflowException
                }

                throw new DriverException(String.Format("Rsql driver: cannot convert SqlDecimal value {0} to decimal, because decimal has only a precision of 28.", this._val.ToString()));
            }

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldFloat : IField
    {
        private SqlDouble _val;

        internal FieldFloat()
        {
            this._val = SqlDouble.Null;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Float;
        }

        public bool IsNULL()
        {
            return this._val.IsNull;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = SqlDouble.Null;
                return;
            }

            // value

            double valdouble = buff.ReadDouble();
            this._val = new SqlDouble(valdouble);
        }

        public object GetValue()
        {
            if (this._val.IsNull)
                return System.DBNull.Value;

            return this._val.Value;
        }

        public object GetSqlValue()
        {
            return this._val;
        }
    }

    internal class FieldDate : IField
    {
        private DateTime _val;   // SqlDatetime is not used because range is from 1753.01.01 to 9999.01.01. Also, SqlDatetime2 is not available.
        private bool _nullFlag;  // if true, value is NULL

        internal FieldDate()
        {
            this._val = DateTime.MinValue;
            this._nullFlag = true;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Date;
        }

        public bool IsNULL()
        {
            return this._nullFlag;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = DateTime.MinValue;
                this._nullFlag = true;
                return;
            }

            // value

            long absoluteDays = buff.ReadUint(); // number of days since 0001.01.01
            this._val = new DateTime(absoluteDays * TimeSpan.TicksPerDay); // pass number of ticks since 1900.01.01

            this._nullFlag = false;
        }

        public object GetValue()
        {
            if (this._nullFlag)
                return System.DBNull.Value;

            return this._val;
        }

        public object GetSqlValue()
        {
            if (this._nullFlag)
                return System.DBNull.Value;

            return this._val; // there is no SqlDate struct, so we return System.DateTime
        }
    }

    internal class FieldTime : IField
    {
        private TimeSpan _val;   // we use it because therre is no SqlTime type
        private bool _nullFlag;  // if true, value is NULL

        internal FieldTime()
        {
            this._val = TimeSpan.Zero;
            this._nullFlag = true;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.Time;
        }

        public bool IsNULL()
        {
            return this._nullFlag;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = TimeSpan.Zero;
                this._nullFlag = true;
                return;
            }

            // value

            int sz = buff.ReadArrayHeader();
            if (sz != 2)
                throw new DriverException("MessagePack: reading a TIME value, but received illegal bytes.");


            long seconds = buff.ReadUint();
            long nanoseconds = buff.ReadUint();

            this._val = new TimeSpan(seconds*TimeSpan.TicksPerSecond + nanoseconds/100); // one tick is 100 ns

            this._nullFlag = false;
        }

        public object GetValue()
        {
            if (this._nullFlag)
                return System.DBNull.Value;

            return this._val;
        }

        public object GetSqlValue()
        {
            if (this._nullFlag)
                return System.DBNull.Value;

            return this._val; // there is no SqlTime struct, so we return System.TimeSpan
        }
    }

    internal class FieldDatetime : IField
    {
        private DateTime _val;   // SqlDatetime is not used because range is from 1753.01.01 to 9999.01.01. Also, SqlDatetime2 is not available.
        private bool _nullFlag;  // if true, value is NULL

        internal FieldDatetime()
        {
            this._val = DateTime.MinValue;
            this._nullFlag = true;
        }

        public SqlDbType Datatype()
        {
            return SqlDbType.DateTime2;
        }

        public bool IsNULL()
        {
            return this._nullFlag;
        }

        public void ReadValue(BufferIn buff)
        {
            if (buff.PeekMsgpType() == MsgpType.NilType)
            {
                buff.ReadNil();
                this._val = DateTime.MinValue;
                this._nullFlag = true;
                return;
            }

            // value

            int sz = buff.ReadArrayHeader();
            if (sz != 3)
                throw new DriverException("MessagePack: reading a DATETIME value, but received illegal bytes.");

            long absoluteDays = buff.ReadUint(); // number of days since 0001.01.01
            long seconds      = buff.ReadUint();
            long nanoseconds  = buff.ReadUint();

            this._val = new DateTime(absoluteDays * TimeSpan.TicksPerDay + seconds * TimeSpan.TicksPerSecond + nanoseconds / 100); // one tick is 100 ns. Pass number of ticks since 1900.01.01

            this._nullFlag = false;
        }

        public object GetValue()
        {
            if (this._nullFlag)
                return System.DBNull.Value;

            return this._val;
        }

        public object GetSqlValue()
        {
            if (this._nullFlag)
                return SqlDateTime.Null;

            return new SqlDateTime(this._val);
        }
    }

}
