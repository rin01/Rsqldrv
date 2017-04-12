using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;


namespace Rsqldrv.SqlClient
{
    internal enum StepOption
    {
        STEP_NEXT_RECORD = 0, // step() will return when next record layout or record data is available
        STEP_EXECUTE_ALL = 1  // step() will read and discard all data coming from the server until end of the batch
    }

    // SqlDataReader executes the batch, and can read the record layout description (column names, data types, etc) and the field values of each record.
    // A SqlDataReader object cannot be reused. It is only constructed by the SqlCommand.ExecuteReader() method.
    public class SqlDataReader : DbDataReader, IDataReader, IDisposable, IDataRecord
    {        
        private string _text = "";
        private SqlConnection _conn; // if conn is null, the SqlDataReader is considered as closed

        private byte _status;
        private string[] _colnameList; // list of column name in lowercase
        private Dictionary<string,int> _colnameMap; // column name to field position in record
        private IField[] _record; // contains fields, which store values in struct SqlInt64, SqlDecimal, etc from System.Data.SqlTypes
        private long _lastRecordCount; // record count for SELECT statement
        private long _lastExecRecordCount; // record count for statements like INSERT, UDDATE, DELETE, etc
        private long _lastRecordAffectedCount; // value of last _lastRecordCount or _lastExecRecordCount
        private long _cumulativeExecRecordCount; // for SqlCommand.ExecuteNonQuery()
        private long _recordsetCount; // number of recordsets in the batch

        private string _messageString = ""; // last message
        private SqlException _sqlException; // only real error, not warning
        private bool _firstScalarFlag;
        private object _firstScalar;
        private long _rc; // return code of batch

        private bool _disposed;

        //===== constructor =====

        internal SqlDataReader(string sqlText, SqlConnection conn)
        {
            this._text = sqlText;
            this._conn = conn;
        }

        //===== preoperties =====

        internal long CumulativeExecRecordCount { get { return this._cumulativeExecRecordCount; } }

        internal object FirstScalar { get { return this._firstScalar; } }

        //===== methods for executing batch and reading records =====

        // Send the SQL batch and execute it.
        //
        // SqlCommand.ExecuteReader() calls this method with StepOption.STEP_NEXT_RECORD:
        //    receive messages from server and stop at first recordset layout (status == RECORD_LAYOUT_AVAILABLE)
        //    or end of batch if no SELECT statement in batch (status == BATCH_END) or if error received (status == BATCH_END).
        //
        // SqlCommand.ExecuteNonQuery() and SqlCommand.ExecuteScalar() call this method with StepOption.STEP_EXECUTE_ALL:
        //    execute until end of batch or error received (status == BATCH_END).
        internal void SendAndExecuteBatch(StepOption stepOption)
        {
            // send the batch to the server

            this._conn.buffout.Reset();
            this._conn.buffout.AppendUlong(ReqType.BATCH);
            this._conn.buffout.AppendString(this._text);
            this._conn.SendBuffout();
            this._status = BatchStatus.BATCH_SENT;

            // receive the messages from the server and process them

            step(stepOption); // process the whole batch successfullly, or stop at first recordset layout, or throw an exception if error received

            // here, we can have:
            //
            //    this._status = BATCH_END with this._sqlException != null            batch ended with error (step() has thrown an exception on the line above)
            //    this._status = RECORD_LAYOUT_AVAILABLE                              record layout is available
            //    this._status = BATCH_END with this._sqlException == null            batch ended successfully

        }

        // Executes the batch until record layout or record data is available, or end of batch if no more recordset.
        // The method ExecuteReader will call step() and reads the record layout if any.
        // Subsequent Read()s will read each record data.
        // If no record is found, it returns at the end of the batch.
        public override bool Read()
        {
            if (this._status == BatchStatus.BATCH_END) // happens only if ExecuteReader() has executed all the batch because no SELECT was found
                return false;

            if (this._status != BatchStatus.RECORD_LAYOUT_AVAILABLE && this._status != BatchStatus.RECORD_DATA_AVAILABLE)
                throw new DriverException(String.Format("SqlDataReader: unexpected error. State should be RECORD_LAYOUT_AVAILABLE or RECORD_DATA_AVAILABLE, but is {0}.", this._status));

            // when status == RECORD_LAYOUT_AVAILABLE, step() below will return false so that    while (myReader.Read())    can exit the loop
            // when status == RECORD_DATA_AVAILABLE,   step() below will return true
            // when status == BATCH_END,               step() below will return false

            return step(StepOption.STEP_NEXT_RECORD); // returns only when record layout is available, or record data is available, or end of batch- Throw exception if error received.
        }

        // Returns true if SqlDataReader has read the layout (description of columns) of a recordset.
        public override bool NextResult()
        {
            return this._status == BatchStatus.RECORD_LAYOUT_AVAILABLE;
        }

        // step reads all the response message sent by the server.
        // It returns when a recordset is reached.
        // This function returns true if a record is available and its column values can be read.
        //
        // Note: if the batch was sent by ExecuteNonQuery(), all statements are executed until the batch terminates.
        private bool step(StepOption option) // returns true if recordset found
        {
            while (true)
            {
                uint response = this._conn.buffin.ReadUint();

                switch (response)
                {
                    case ResType.RECORD_LAYOUT:
                        //Console.WriteLine("RECORD_LAYOUT");
                        // create colname list and map

                        this._colnameList = this.createColnameList(); // create list

                        this._colnameMap = new Dictionary<string,int>(this._colnameList.Length); // create map
                        for (int i=0; i<this._colnameList.Length; i++)
                        {
                            string name = this._colnameList[i];
                            if (name == "")
                                continue;

                            if (this._colnameMap.ContainsKey(name) == false)
                                this._colnameMap[name] = i;
                            else
                                this._colnameMap.Remove(name); // ambiguous column name
                        }

                        // create record

                        this._record = this.createRow();

                        this._lastRecordCount = 0;
                        this._recordsetCount++;
                        this._status = BatchStatus.RECORD_LAYOUT_AVAILABLE;

                        // return if STEP_NEXT_RECORD

                        if (option == StepOption.STEP_NEXT_RECORD)
                            return false;

                        continue;
                       
                    case ResType.RECORD_DATA: // a record is available
                        //Console.WriteLine("RECORD_DATA");
                        // fill record

                        this.fillRowWithValues(this._record);

                        this._lastRecordCount++;
                        this._status = BatchStatus.RECORD_DATA_AVAILABLE;

                        if (this._firstScalarFlag == false)
                        {
                            this._firstScalar = this.GetValue(0);
                            if (this._firstScalar is byte[])
                                throw new DriverException("SqlDataReader: ExecuteScalar() doesn't accept a byte array for first column of first row.");

                            this._firstScalarFlag = true;
                        }

                        if (option == StepOption.STEP_NEXT_RECORD)
                            return true;

                        continue;
                        
                    case ResType.RECORD_FINISHED: // record count is available
                        //Console.WriteLine("RECORD_FINISHED");
                        long recordCount = this._conn.buffin.ReadLong();

                        if (recordCount != this._lastRecordCount)
                            throw new DriverException("SqlDataReader: count of record sent by server and received by client don't match.");

                        // discard record

                        this._colnameList = null;
                        this._colnameMap = null;
                        this._record = null;
                        this._lastRecordCount = recordCount;
                        this._lastRecordAffectedCount = recordCount;

                        this._status = BatchStatus.RECORD_END;

                        continue;
                        
                    case ResType.EXECUTION_FINISHED: // if SET NOCOUNT ON, INSERT etc statements don't send this information
                        //Console.WriteLine("EXECUTION_FINISHED");
                        this._lastExecRecordCount = this._conn.buffin.ReadLong();
                        this._lastRecordAffectedCount = this._lastExecRecordCount;
                        this._cumulativeExecRecordCount += this._lastExecRecordCount;

                        continue;

                    case ResType.PRINT:
                        //Console.WriteLine("PRINT");
                        // create row

                        IField[] dummyRecord = this.createRow();
                        this.fillRowWithValues(dummyRecord); // read row but ignore it

                        continue;

                    case ResType.MESSAGE:
                        //Console.WriteLine("MESSAGE");

                        this._messageString = this._conn.buffin.ReadString();

                        continue;

                    case ResType.ERROR:
                        //Console.WriteLine("ERROR");

                        SqlError sqlError = SqlError.ReadSqlError(this._conn.buffin, this._conn.serverName);
                        SqlException sqlException = new SqlException(sqlError);

                        // the server will send ResType.BATCH_END after it has sent this error.
                        // if state == 127 (only THROW or ERROR_SERVER_ABORT can generate it), server also closed the connection.

                        if (sqlException.Class > 10) // real error (warnings have severity <=10 and are discarded, e.g. DROP USER: user not found)
                            this._sqlException = sqlException;

                        continue;

                    case ResType.BATCH_END: // batch is finished, no more messages are expected from server for this batch
                        //Console.WriteLine("BATCH_END");
                        this._rc = this._conn.buffin.ReadLong();
                        this._status = BatchStatus.BATCH_END;

                        if (this._sqlException != null) // if server error detected, throw it
                            throw this._sqlException;

                        return false;

                    default:
                        throw new DriverException("SqlDataReader: unknown response type received from server.");
                }

            }
        }

        //===== helper methods for reading records =====

        private string[] createColnameList()
        {
            int rowSize = this._conn.buffin.ReadArrayHeader();

            string[] colnameList = new string[rowSize];
            for (int i = 0; i<rowSize; i++)
            {
                string colname = this._conn.buffin.ReadString();
                colnameList[i] = colname.ToLower();
            }

            return colnameList;
        }

        private IField[] createRow()
        {
            // create row and read field datatypes

            int rowSize = this._conn.buffin.ReadArrayHeader();

            IField[] row = new IField[rowSize];
            for (int i = 0; i<rowSize; i++)
            {
                IField field = this.newField();
                row[i] = field;
            }

            return row;
        }

        // new_fields returns a IField object, created by reading from messagepack Reader. It returns e.g. *Int, *Numeric, *Date, etc.
        private IField newField()
        {
            int sz = this._conn.buffin.ReadArrayHeader(); // each datatype information is contained in an array
            int u = this._conn.buffin.ReadInt(); // read datatype

            uint precision;
            uint scale;
            bool fixlen;

	        switch (u)
            {
	            case Dtype.VOID:
		            Assert.check(sz == 1);
		            return new FieldVoid();

	            case Dtype.BOOLEAN:
		            Assert.check(sz == 1);
		            return new FieldBoolean();

	            case Dtype.VARBINARY:
		            Assert.check(sz == 2);
		            precision = this._conn.buffin.ReadUint();

		            return new FieldVarbinary(precision);

	            case Dtype.VARCHAR:
		            Assert.check(sz == 3);
		            precision = this._conn.buffin.ReadUint();
		            fixlen = this._conn.buffin.ReadBool();

		            return new FieldVarchar(precision, fixlen);

	            case Dtype.BIT:
		            Assert.check(sz == 1);
		            return new FieldBit();

	            case Dtype.TINYINT:
		            Assert.check(sz == 1);
		            return new FieldTinyint();

	            case Dtype.SMALLINT:
		            Assert.check(sz == 1);
		            return new FieldSmallint();

	            case Dtype.INT:
		            Assert.check(sz == 1);
		            return new FieldInt();

	            case Dtype.BIGINT:
		            Assert.check(sz == 1);
		            return new FieldBigint();

	            case Dtype.MONEY:
		            Assert.check(sz == 3);
                    precision = this._conn.buffin.ReadUint();
		            scale = this._conn.buffin.ReadUint();

		            return new FieldMoney(precision, scale);

	            case Dtype.NUMERIC:
		            Assert.check(sz == 3);
                    precision = this._conn.buffin.ReadUint();
		            scale = this._conn.buffin.ReadUint();

		            return new FieldNumeric(precision, scale);

	            case Dtype.FLOAT:
		            Assert.check(sz == 1);
		            return new FieldFloat();

	            case Dtype.DATE:
		            Assert.check(sz == 1);
		            return new FieldDate();

	            case Dtype.TIME:
		            Assert.check(sz == 1);
		            return new FieldTime();

	            case Dtype.DATETIME:
		            Assert.check(sz == 1);
		            return new FieldDatetime();

	            default:
                    throw new DriverException("SqlDataReader: unknown datatype received");
	        }
        }

        // Fill_row_with_values fills in values into row fields, from a messagepack Reader.
        private void fillRowWithValues(IField[] row)
        {
            // read field values and fill-in row

            int rowSize = this._conn.buffin.ReadArrayHeader();
            if (rowSize != row.Length)
                throw new DriverException("SqlDataReader: number of values received != column count");

            foreach (IField field in row)
                field.ReadValue(this._conn.buffin);
		}

        //===== implementation of properties =====

        public SqlConnection Connection
        {
            get { return this._conn; }
        }

        public override int Depth { get { return 0; } } // always return 0

        public override int FieldCount
        {
            get
            {
                if (this._conn == null)
                    throw new NotSupportedException("SqlDataReader: No connection.");

                if (this._record == null)
                    return 0;

                return this._record.Length;
            }
        }

        public override bool HasRows { get { throw new DriverException("SqlDataReader: property HasRows is not implemented."); } }

        public override bool IsClosed { get { return this._conn == null; } }

        public override Object this[int i]
        {
            get
            {
                return this._record[i].GetValue(); // GetValue() returns DBnull for NULL values
            }
        }

        public override Object this[string name]
        {
            get
            {
                int colOrdinal = this._colnameMap[name.ToLower()];

                return this._record[colOrdinal].GetValue(); // GetValue() returns DBnull for NULL values
            }
        }

        public override int RecordsAffected
        {
            get
            {
                return (int)this._lastRecordAffectedCount;
            }
        }

        //===== implementation of methods for record =====

        public override System.Collections.IEnumerator GetEnumerator()
        {
            return this._record.GetEnumerator();
        }

        public override string GetName(int i)
        {
            return this._colnameList[i];
        }

        public override int GetOrdinal(string name)
        {
            return this._colnameMap[name.ToLower()];
        }

        public override DataTable GetSchemaTable()
        {
            throw new DriverException("SqlDataReader: method GetSchemaTable is not implemented.");
        }

        //===== implementation of methods that returns field values as System .Net types =====

        public override char GetChar(int i)
        {
            throw new DriverException("SqlDataReader: method GetChar is not implemented.");
        }

        public override long GetBytes(int i, long dataIndex, byte[] buffer, int bufferIndex, int length)
        {
            throw new DriverException("SqlDataReader: method GetBytes is not implemented. Use (byte[])myreader[i] instead.");
        }

        public override long GetChars(int i, long dataIndex, char[] buffer, int bufferIndex, int length)
        {
            throw new DriverException("SqlDataReader: method GetChars is not implemented.");
        }

        public override string GetString(int i)
        {
            return (string)this._record[i].GetValue();
        }

        public override bool GetBoolean(int i)
        {
            return (bool)this._record[i].GetValue();
        }

        public override byte GetByte(int i)
        {
            return (byte)this._record[i].GetValue();
        }

        public override short GetInt16(int i)
        {
            return (short)this._record[i].GetValue();
        }

        public override int GetInt32(int i)
        {
            return (int)this._record[i].GetValue();
        }

        public override long GetInt64(int i)
        {
            return (long)this._record[i].GetValue();
        }

        public override decimal GetDecimal(int i)
        {
            return (decimal)this._record[i].GetValue();
        }

        public override float GetFloat(int i)
        {
            return (float)(double)this._record[i].GetValue(); // rsql only have double precision floating point value
        }

        public override double GetDouble(int i)
        {
            return (double)this._record[i].GetValue();
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime)this._record[i].GetValue();
        }

        public virtual TimeSpan GetTimeSpan(int i)
        {
            return (TimeSpan)this._record[i].GetValue();
        }

        public virtual DateTimeOffset GetDateTimeOffset(int i)
        {
            throw new DriverException("SqlDataReader: method DateTimeOffset is not implemented.");
        }

        public override Guid GetGuid(int i)
        {
            throw new DriverException("SqlDataReader: method GetGuid is not implemented.");
        }

        public override object GetValue(int i)
        {
            return this._record[i].GetValue();
        }

        public override int GetValues(object[] values)
        {
            for (int i = 0; i < this._record.Length; i++)
            {
                values[i] = this._record[i].GetValue();
            }

            return this._record.Length;
        }

        public override string GetDataTypeName(int i)
        {
            return this._record[i].GetValue().GetType().ToString(); // TODO verifier resultat
        }

        public override Type GetFieldType(int i)
        {
            return this._record[i].GetValue().GetType(); // TODO verifier resultat
        }

        public override bool IsDBNull(int i)
        {
            return this._record[i].IsNULL();
        }

        //===== implementation of methods that returns field values as System.Data.SqlTypes .Net types =====

        public virtual SqlBytes GetSqlBytes(int i)
        {
            throw new DriverException("SqlDataReader: method GetSqlBytes is not implemented.");
        }

        public virtual SqlBinary GetSqlBinary(int i)
        {
            return (SqlBinary)this._record[i].GetSqlValue();
        }

        public virtual SqlChars GetSqlChars(int i)
        {
            throw new DriverException("SqlDataReader: method GetSqlChars is not implemented.");
        }

        public virtual SqlString GetSqlString(int i)
        {
            return (SqlString)this._record[i].GetSqlValue();
        }

        public virtual SqlBoolean GetSqlBoolean(int i)
        {
            return (SqlBoolean)this._record[i].GetSqlValue();
        }

        public virtual SqlByte GetSqlByte(int i)
        {
            return (SqlByte)this._record[i].GetSqlValue();
        }

        public virtual SqlInt16 GetSqlInt16(int i)
        {
            return (SqlInt16)this._record[i].GetSqlValue();
        }

        public virtual SqlInt32 GetSqlInt32(int i)
        {
            return (SqlInt32)this._record[i].GetSqlValue();
        }

        public virtual SqlInt64 GetSqlInt64(int i)
        {
            return (SqlInt64)this._record[i].GetSqlValue();
        }

        public virtual SqlMoney GetSqlMoney(int i)
        {
            return (SqlMoney)this._record[i].GetSqlValue();
        }

        public virtual SqlDecimal GetSqlDecimal(int i)
        {
            return (SqlDecimal)this._record[i].GetSqlValue();
        }

        public virtual SqlSingle GetSqlSingle(int i)
        {
            return ((SqlDouble)this._record[i].GetSqlValue()).ToSqlSingle(); // rsql only have double precision floating point value
        }

        public virtual SqlDouble GetSqlDouble(int i)
        {
            return (SqlDouble)this._record[i].GetSqlValue();
        }

        public virtual SqlDateTime GetSqlDateTime(int i)
        {
            return (SqlDateTime)this._record[i].GetSqlValue();
        }

        public virtual SqlGuid GetSqlGuid(int i)
        {
            throw new DriverException("SqlDataReader: method GetSqlGuid is not implemented.");
        }

        public virtual object GetSqlValue(int i)
        {
            return this._record[i].GetSqlValue();
        }

        public virtual int GetSqlValues(object[] values)
        {
            for (int i = 0; i < this._record.Length; i++)
            {
                values[i] = this._record[i].GetSqlValue();
            }

            return this._record.Length;
        }

        public override void Close()
        {
            this.Dispose();
        }

        protected override void Dispose(bool disposing)
        {
            if (this._disposed)
                return;

            if (disposing)
            {
                this._text = "";
                this._conn = null;
                this._status = 0;
                this._colnameList = null;
                this._colnameMap = null;
                this._record = null;
                this._lastRecordCount = 0;
                this._lastExecRecordCount = 0;
                this._lastRecordAffectedCount = 0;
                this._cumulativeExecRecordCount = 0;
                this._recordsetCount = 0;

                this._messageString = "";
                this._sqlException = null;
                this._firstScalarFlag = false;
                this._firstScalar = null;
                this._rc = 0;
            }

            base.Dispose(disposing); // dispose DbCommand
            this._disposed = true;
        }


    }
}
