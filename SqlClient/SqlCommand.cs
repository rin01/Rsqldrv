using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.ComponentModel;

// TODO
//    implement CommandTimeout behaviour


namespace Rsqldrv.SqlClient
{
    [System.ComponentModel.DesignerCategory("Code")]

    public sealed class SqlCommand : DbCommand, ICloneable
    {
        static readonly int DEFAULT_COMMAND_TIMEOUT = 30;

        private CommandType _textType = CommandType.Text; // only Text is allowed
        private string _text = ""; // SQL text of the batch to execute. It may contain parameter placeholders, e.g. @name.
        private SqlConnection _conn;
        private int _commandTimeout; // in seconds, not used yet

        private SqlParameterCollection _paramColl; // collection of parameters. It should only be accessed through Parameters property, which creates it if needed.

        private bool _disposed;

        //===== constructors =====

        public SqlCommand() { }

        public SqlCommand(string sqlText) : this(sqlText, null)
        {
        }

        public SqlCommand(string sqlText, SqlConnection conn)
        {
            this._text = sqlText;
            this._conn = conn;
        }

        //===== API properties =====

        public override string CommandText
        {
            get { return this._text; }
            set { this._text = value; }
        }

        public override CommandType CommandType
        {
            get { return this._textType; }
            set
            {
                if (value != CommandType.Text)
                    throw new DriverException("SqlCommand: only CommandType.Text is allowed.");

                this._textType = value;
            }
        }

        public override int CommandTimeout
        {
            get { return this._commandTimeout; }
            set { this._commandTimeout = value; }
        }

        public new SqlConnection Connection
        {
            get { return this._conn; }
            set
            {
                if (this._disposed)
                    throw new DriverException("SqlCommand: cannot set the connection, as the SqlCommand object has been disposed.");

                if (this._conn != null)
                    throw new DriverException("SqlCommand: a connection already exists in the SqlCommand object.");                 

                this._conn = value;
            }
        }

        public new SqlParameterCollection Parameters
        {
            get
            {
                if (this._paramColl == null)
                    this._paramColl = new SqlParameterCollection();

                return this._paramColl;
            }
        }

        //===== API methods =====

        public new SqlParameter CreateParameter()
        {
            return new SqlParameter();
        }

        public void ResetCommandTimeout()
        {
            this._commandTimeout = DEFAULT_COMMAND_TIMEOUT;
        }

        public override void Prepare()
        {
            // do nothing. Rsql doesn't create stored procedure for the batch.
        }

        // Execute the whole batch.
        // Return cumulative number of inserted/updated/deleted records for all INSERT/UPDATE/DELETE statements in the batch.
        public override int ExecuteNonQuery()
        {
            SqlDataReader reader = null;
            string finalText = this.Parameters.GetAllDECLAREstring() + this._text;

            try
            {
                reader = new SqlDataReader(finalText, this._conn);
                reader.SendAndExecuteBatch(StepOption.STEP_EXECUTE_ALL);
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                    reader = null;
                }
            }

            return (int)reader.CumulativeExecRecordCount; // available even after reader.Dispose()
        }

        // Execute the whole batch and return the value of first column in first record.
        // NOTE: MS SQL Server reads the first record of SELECT and discard all the remaining statements, and even errors !
        //       But Rsql raises an Exception if the server returns an error.
        public override object ExecuteScalar()
        {
            SqlDataReader reader = null;
            string finalText = this.Parameters.GetAllDECLAREstring() + this._text;

            try
            {
                reader = new SqlDataReader(finalText, this._conn);
                reader.SendAndExecuteBatch(StepOption.STEP_EXECUTE_ALL);
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                    reader = null;
                }
            }

            return reader.FirstScalar; // available even after reader.Dispose()
        }

        public new SqlDataReader ExecuteReader()
        {
            SqlDataReader reader = null;
            string finalText = this.Parameters.GetAllDECLAREstring() + this._text;

            reader = new SqlDataReader(finalText, this._conn);
            reader.SendAndExecuteBatch(StepOption.STEP_NEXT_RECORD);

            return reader;
        }

        // called asynchronously from another thread.
        public override void Cancel()
        {
            try
            {
                if (this._conn != null)
                    this._conn.SendCancel();
            }
            catch
            {
                // discard all exceptions that can occur when sending cancel message
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (this._disposed)
                return;

            if (disposing)
            {
                this._text = "";
                this._conn = null;
                this._paramColl = null;
            }

            base.Dispose(disposing); // dispose DbCommand
            this._disposed = true;
        }

        //===== various protected override methods =====

        protected override DbConnection DbConnection
        {
            get { return this._conn; }
            set { this._conn = (SqlConnection)value; }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { return (DbParameterCollection)this.Parameters; }
        }

        protected override DbTransaction DbTransaction
        {
            get { throw new DriverException("SqlCommand: DbTransaction property is not supported."); }
            set { throw new DriverException("SqlCommand: DbTransaction property is not supported."); }
        }

        public override bool DesignTimeVisible
        {
            get { throw new DriverException("SqlCommand: DesignTimeVisible property is not supported."); }
            set { throw new DriverException("SqlCommand: DesignTimeVisible property is not supported."); }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { throw new DriverException("SqlCommand: UpdatedRowSource property is not supported."); }
            set { throw new DriverException("SqlCommand: UpdatedRowSource property is not supported."); }
        }

        protected override DbParameter CreateDbParameter()
        {
            return (DbParameter)new SqlParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new DriverException("SqlCommand: ExecuteDbDataReader method is not supported.");
        }

        object ICloneable.Clone()
        {
            throw new DriverException("SqlCommand: Clone method is not supported.");
        }

    }
}
