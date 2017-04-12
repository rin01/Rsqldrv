using System;
using System.Globalization;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;

namespace Rsqldrv.SqlClient
{
    public sealed class SqlParameter : DbParameter, IDbDataParameter, IDataParameter, ICloneable
    {
        private string    _name = ""; // name of parameter. Must start with @.
        private SqlDbType _sqlDbType = SqlDbType.Variant; // Int, BigInt, Decimal, Date, etc. As 0 is Bigint, we use Variant as invalid type.

        private int _size;      // for SqlDbType.Varbinary, Char, NChar, Varchar, NVarchar
        private int _precision; // for SqlDbType.Decimal
        private int _scale;     // for SqlDbType.Decimal
        private object _value;  // value of parameter

        private int _sizeFixed;      // size set by property Size
        private int _precisionFixed; // precision set by property Precision
        private int _scaleFixed;     // scale set by property Scale

        //===== constructors =====

        public SqlParameter()
        {
        }

        public SqlParameter(string parameterName, SqlDbType sqlDbType)
        {
            checkParameterNameValidity(parameterName); // throw exception if error

            this._name = parameterName;
            this._sqlDbType = sqlDbType;
        }

        public SqlParameter(string parameterName, SqlDbType sqlDbType, int size) : this(parameterName, sqlDbType)
        {
            this._sizeFixed = size;
        }

        //===== API properties =====

        public override DbType DbType
        {
            get { throw new DriverException("SqlParameter: DbType property is not supported."); }
            set { throw new DriverException("SqlParameter: DbType property is not supported."); }
        }

        public override ParameterDirection Direction
        {
            get { throw new DriverException("SqlParameter: Direction property is not supported."); }
            set { throw new DriverException("SqlParameter: Direction property is not supported."); }
        }

        public override bool IsNullable
        {
            get { throw new DriverException("SqlParameter: IsNullable property is not supported."); }
            set { throw new DriverException("SqlParameter: IsNullable property is not supported."); }
        }

        public override string SourceColumn
        {
            get { throw new DriverException("SqlParameter: SourceColumn property is not supported."); }
            set { throw new DriverException("SqlParameter: SourceColumn property is not supported."); }
        }

        public override bool SourceColumnNullMapping
        {
            get { throw new DriverException("SqlParameter: SourceColumnNullMapping property is not supported."); }
            set { throw new DriverException("SqlParameter: SourceColumnNullMapping property is not supported."); }
        }

        public SqlDbType SqlDbType
        {
            get { return this._sqlDbType; }
            set { this._sqlDbType = value; }
        }

        public override string ParameterName
        {
            get { return this._name; }

            set {
                checkParameterNameValidity(value); // throw exception if error

                this._name = value;
            }
        }

        public override int Size
        {
            get { return this._sizeFixed == 0 ? this._size : this._sizeFixed; }
            set { this._sizeFixed = value; }
        }

        public override byte Precision
        {
            get { return (byte)(this._precisionFixed == 0 ? this._precision : this._precisionFixed); }
            set
            {
                this._precisionFixed = value;
                throw new DriverException(String.Format("SqlParameter {0}: set Precision property is not supported.", this._name));
            }
        }

        public override byte Scale
        {
            get { return (byte)(this._scaleFixed == 0 ? this._scale : this._scaleFixed); }
            set
            {
                this._scaleFixed = value;
                throw new DriverException(String.Format("SqlParameter {0}: set Scale property is not supported.", this._name));
            }
        }

        public override object Value
        {
            get { return this._value; }
            set
            {
                this._value = value;

                // setting the size, precision and scale here is just for information.
                // It is not useful as it will be recomputed in the function GetDECLAREstring.

                if (value is byte[])
                {
                    this._size = ((byte[])value).Length;
                    return;
                }
                else if (value is string)
                {
                    this._size = ((string)value).Length;
                    return;
                }
                else if (value is decimal)
                {
                    SqlDecimal valdec = new SqlDecimal((decimal)value);
                    this._precision = valdec.Precision;
                    this._scale = valdec.Scale;
                    return;
                }
            }
        }

        //===== API methods =====

        public override void ResetDbType()
        {
            throw new DriverException("SqlParameter: ResetDbType method is not supported.");
        }

        object ICloneable.Clone()
        {
            throw new DriverException("SqlParameter: Clone method is not supported.");
        }

        public override string ToString()
        {
            return String.Format("param {0}:{1}", this._name, Convert.IsDBNull(this._value) ? "<NULL>" : this._value);
        }

        public string GetDECLAREstring()
        {
            const int RSQL_DECIMAL_MAX_PRECISION = 34; // SQL Server max precision for NUMERIC

            byte[] barray;
            string s;
            bool mybool;
            decimal dec;
            SqlDecimal sqldec;
            double dbl;
            DateTime dt;
            TimeSpan tspan;
            string valStr = "NULL"; // default
            CultureInfo ci = CultureInfo.InvariantCulture;

            if (String.IsNullOrEmpty(this._name))
                throw new DriverException("SqlParameter: parameter name is null or empty.");

            if (this._value == null)
                throw new DriverException(String.Format("SqlParameter {0}: value null is not allowed.", this._name));

            switch (this._sqlDbType)
            {
                case SqlDbType.VarBinary:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        if (!(this._value is byte[]))
                            throw new DriverException(String.Format("SqlParameter {0}: only byte array can be accepted as VarBinary.", this._name));

                        barray = (byte[])this._value;
                        this._size = barray.Length;
                        valStr = "0x" + BitConverter.ToString(barray).Replace("-", "");
                    }
                    if (this._size == 0)
                        this._size = 1;

                    return String.Format("DECLARE {0} VARBINARY({1}) = {2};", this._name, this._sizeFixed == 0 ? this._size : this._sizeFixed, valStr);

                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Char:
                case SqlDbType.VarChar:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        s = Convert.ToString(this._value); // current locale
                        this._size = s.Length;
                        valStr = "'" + s.Replace("'", "''") + "'"; // escape single quotes by doubling them
                    }
                    if (this._size == 0)
                        this._size = 1;

                    return String.Format("DECLARE {0} VARCHAR({1}) = {2};", this._name, this._sizeFixed == 0 ? this._size : this._sizeFixed, valStr);

                case SqlDbType.Bit:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        mybool = Convert.ToBoolean(this._value); // current locale
                        valStr = "0";
                        if (mybool)
                            valStr = "1";
                    }
                    return String.Format("DECLARE {0} BIT = {1};", this._name, valStr);

                case SqlDbType.TinyInt:
                    if (Convert.IsDBNull(this._value) == false)
                        valStr = Convert.ToString(Convert.ToByte(this._value), ci); // current locale

                    return String.Format("DECLARE {0} TINYINT = {1};", this._name, valStr);

                case SqlDbType.SmallInt:
                    if (Convert.IsDBNull(this._value) == false)
                        valStr = Convert.ToString(Convert.ToInt16(this._value), ci); // current locale

                    return String.Format("DECLARE {0} SMALLINT = {1};", this._name, valStr);

                case SqlDbType.Int:
                    if (Convert.IsDBNull(this._value) == false)
                        valStr = Convert.ToString(Convert.ToInt32(this._value), ci); // current locale

                    return String.Format("DECLARE {0} INT = {1};", this._name, valStr);

                case SqlDbType.BigInt:
                    if (Convert.IsDBNull(this._value) == false)
                        valStr = Convert.ToString(Convert.ToInt64(this._value), ci); // current locale

                    return String.Format("DECLARE {0} BIGINT = {1};", this._name, valStr);

                case SqlDbType.SmallMoney:
                case SqlDbType.Money:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        if (isBinaryFloatValueToDecimal(this._sqlDbType, this._value))
                            throw new DriverException(String.Format("SqlParameter {0}: cannot cast double or float value to Decimal. Use a decimal value, or a literal with M suffix.", this._name));

                        dec = Convert.ToDecimal(this._value); // current locale
                        valStr = dec.ToString("0.####", ci); // 4 # because it is the precision of the SQL MONEY datatype
                    }
                    return String.Format("DECLARE {0} MONEY = {1};", this._name, valStr);

                case SqlDbType.Decimal:
                    this._precision = 1;
                    this._size = 0;
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        if (isBinaryFloatValueToDecimal(this._sqlDbType, this._value))
                            throw new DriverException(String.Format("SqlParameter {0}: cannot cast double or float value to Decimal. Use a decimal value, or a literal with M suffix.", this._name));

                        dec = Convert.ToDecimal(this._value); // current locale
                        sqldec = new SqlDecimal(dec);
                        this._precision = sqldec.Precision;
                        this._scale = sqldec.Scale;

                        if (this._precision > RSQL_DECIMAL_MAX_PRECISION) // max precision for numeric is 34 on rsql
                        {
                            this._scale = this._scale - (this._precision - RSQL_DECIMAL_MAX_PRECISION);
                            if (this._scale < 0)
                                this._scale = 0;
                            this._precision = RSQL_DECIMAL_MAX_PRECISION;
                        }

                        if (this._precision == 0)
                            this._precision = 1;

                        valStr = dec.ToString("0.############################", ci); // 28 # because it is the precision of .Net decimal type
                    }

                    // NOTE: for Microsoft, a value of 0 for _precisionFixed means that the value of precision is _precision.
                    //           Also, a value of 0 for _scaleFixed means that the value of scale is _scale.
                    //           It is completely stupid behaviour, because param.Scale = 0 has no effect, as the scale will be the actual scale of the decimal number and not 0 as specified !
                    //           Also, setting the Scale has no effect on Precision. So, increasing the Scale will make the server raise an error, as the number doesn't fit in the created TSLQ variable any more.
                    //           I prefer to forbid the use of Precision and Scale, instead of letting this behaviour surprise the user !
                    //           The behaviour of Microsoft is given by the line below:
                    // return String.Format("DECLARE {0} NUMERIC({1}, {2}) = {3};", this._name, this._precisionFixed == 0 ? this._precision : this._precisionFixed, this._scaleFixed == 0 ? this._scale : this._scaleFixed, valStr);

                    return String.Format("DECLARE {0} NUMERIC({1}, {2}) = {3};", this._name, this._precision, this._scale, valStr);

                case SqlDbType.Real:  // single precision
                case SqlDbType.Float: // double precision
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        dbl = Convert.ToDouble(this._value); // current locale
                        valStr = dbl.ToString("R", ci); // R is the best format for double
                    }
                    return String.Format("DECLARE {0} FLOAT = {1};", this._name, valStr);

                case SqlDbType.Date:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        dt = Convert.ToDateTime(this._value); // current locale
                        valStr = dt.ToString(@"\'yyyy\-MM\-dd\'", ci);
                    }
                    return String.Format("DECLARE {0} DATE = {1};", this._name, valStr);

                case SqlDbType.Time:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        if (this._value is TimeSpan)
                            tspan = (TimeSpan)this._value;
                        else if (this._value is string)
                            tspan = TimeSpan.Parse((string)this._value); // current locale
                        else
                            throw new DriverException(String.Format("SqlParameter {0}: only TimeSpan and string value can be converted to Time.", this._name));

                        valStr = tspan.ToString(@"hh\:mm\:ss\.FFFFFFF", ci); // F doesn't write trailing 0s
                        if (valStr.EndsWith(".")) // because F specifier doesn't delete the preceding dot if fractional part is 0 (it does so for DateTime, though)
                            valStr = valStr.Substring(0, valStr.Length - 1); // remove trailing dot
                        valStr = "'" + valStr + "'";
                    }
                    return String.Format("DECLARE {0} TIME = {1};", this._name, valStr);

                case SqlDbType.SmallDateTime:
                case SqlDbType.DateTime:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        dt = Convert.ToDateTime(this._value); // current locale
                        valStr = dt.ToString(@"\'yyyy'-'MM'-'ddTHH':'mm':'ss\.FFFFFFF\'", ci); // F doesn't write trailing 0s
                    }
                    return String.Format("DECLARE {0} DATETIME = {1};", this._name, valStr);

                case SqlDbType.DateTime2:
                    if (Convert.IsDBNull(this._value) == false)
                    {
                        dt = Convert.ToDateTime(this._value); // current locale
                        valStr = dt.ToString(@"\'yyyy'-'MM'-'ddTHH':'mm':'ss\.FFFFFFF\'", ci); // F doesn't write trailing 0s
                    }
                    return String.Format("DECLARE {0} DATETIME2 = {1};", this._name, valStr);

                default:
                    throw new DriverException(String.Format("SqlParameter {0}: unknown SqlDbType.", this._name));
            }
        }

        //===== helper functions =====

        private static bool isBinaryFloatValueToDecimal(SqlDbType sqlDbType, object value){

            if (sqlDbType == SqlDbType.SmallMoney || sqlDbType == SqlDbType.Money || sqlDbType == SqlDbType.Decimal)
            {
                if (value is float || value is double)
                    return true; // conversion from a binary floating point value to 10-base decimal is forbidden
            }

            return false;
        }

        private static void checkParameterNameValidity(string parameterName)
        {
            if (String.IsNullOrEmpty(parameterName))
                throw new DriverException("SqlParameter: parameter name cannot be null or empty.");

            if (parameterName.StartsWith("@") == false)
                throw new DriverException(String.Format("SqlParameter: parameter name \"{0}\" invalid. Must start with @.", parameterName));

            if (parameterName.StartsWith("@@") == true)
                throw new DriverException(String.Format("SqlParameter: parameter name \"{0}\" invalid. Cannot start with @@.", parameterName));

            if (parameterName.Length < 2)
                throw new DriverException(String.Format("SqlParameter: parameter name \"{0}\" invalid. Length should be >= 2.", parameterName));
        }
    }
}
