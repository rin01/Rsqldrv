using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Rsqldrv.Msgp;
using System.Data.Common;
using System.Data;
using System.Timers;

namespace Rsqldrv.SqlClient
{
    [System.ComponentModel.DesignerCategory("Code")]

    public sealed class SqlConnection : DbConnection, ICloneable
    {
        static internal readonly int DEFAULT_SERVER_PORT = 7777;
        static internal readonly int KEEPALIVE_INTERVAL = 20; // in seconds
        static internal readonly int CONNECTION_TIMEOUT = 15; // in seconds

        static private byte[] _keepaliveByte = {(byte)ReqType.KEEPALIVE};
        static private byte[] _cancelByte = { (byte)ReqType.CANCEL };

        //===== fields =====

        private string _connString = "";

        private string _serverAddr = "";
        private int _serverPort;
        private string _login = "";        // in lower case
        private string _password = "";     // in original case
        private string _database = "";     // in lower case

        private int _keepaliveInterval;  // in seconds. By default, 20 seconds.
        private System.Timers.Timer _timer;

        private int _connectionTimeout; // in seconds, used only when opening the connection

        private TcpClient _tcpClient;
        private Socket _socket; // socket from _tcpClient
        private Object _sendLock; // mutex to serialize Send operations on socket
        private BufferOut _buffout; // message to send to the server
        private BufferIn _buffin; // reading stream from the server

        internal SqlTransaction _transaction; // set by BeginTransaction(), set to null by transaction Commit() or Rollback()

        private ConnectionState _state = ConnectionState.Closed;
        private bool _disposed = false; // when the connection is disposed, it cannot be reopened any more

        //===== constructors =====

        public SqlConnection()
        {
            this._keepaliveInterval = KEEPALIVE_INTERVAL; // in seconds, default value
            this._connectionTimeout = CONNECTION_TIMEOUT; // in seconds, default value
        }

        public SqlConnection(string connectionString) : this()
        {
            this.eatConnectionString(connectionString);
        }

        //===== properties =====

        internal string serverName { get { return String.Format("{0}:{1}", this._serverAddr, this._serverPort); } }

        internal BufferOut buffout { get { return this._buffout; } }

        internal BufferIn buffin { get { return this._buffin; } }

        public override string ConnectionString
        {
            get { return this._connString; }
            set { this._connString = value; this.eatConnectionString(value); }
        }

        public override int ConnectionTimeout { get { return this._connectionTimeout; } }

        public override string Database { get { throw new DriverException("SqlConnection: Database property is not supported."); } }

        public override string DataSource { get { return String.Format("{0}:{1}", this._serverAddr, this._serverPort); } }

        public override string ServerVersion { get { return "version unknown"; } }

        public override ConnectionState State { get { return this._state; } }

        //===== API methods =====

        public override void Open()
        {
            if (this._disposed == true)
                throw new DriverException("SqlConnection: reopen a closed SqlConnection is not allowed.");

            if (this._login == "" || this._password == "")
                throw new DriverException("SqlConnection: login and password cannot be empty string.");

            //--- connect ---

            this._tcpClient = new TcpClient(this._serverAddr, this._serverPort);
            this._socket = this._tcpClient.Client;
            this._sendLock = new Object();
            this._buffout = new BufferOut();
            this._buffin = new BufferIn(this._socket);

            this._timer = new System.Timers.Timer(KEEPALIVE_INTERVAL * 1000); // in ms
            this._timer.Elapsed += new ElapsedEventHandler((sender, args) => SendKeepAlive(this));
            this._timer.AutoReset = false;
            this._timer.Enabled = true;

            //--- send authentication info ---

            Dictionary<string, object> authMessage = new Dictionary<string, object>()
            {
                {"login_name", this._login},
                {"password",   this._password },
                {"database",   this._database } // database may be empty string
            };

            this._buffout.Reset();
            this._buffout.AppendUlong(ReqType.AUTH);
            this._buffout.AppendMapStrSimpleType(authMessage);
            this.SendBuffoutWithTimeout(this._connectionTimeout);

            //--- read authentication response ---

            uint responseType = this._buffin.ReadUint();

            if (responseType != ResType.LOGIN_SUCCESS)
                throw new DriverException("SqlConnection: login failed");
        }

        public override void ChangeDatabase(string database)
        {
            // this method should send a "USE database;" batch to the server

            throw new DriverException("SqlConnection: ChangeDatabase method is not supported.");
        }

        object ICloneable.Clone()
        {
            throw new DriverException("SqlConnection: Clone method is not supported.");
        }

        protected override DbCommand CreateDbCommand()
        {
            return (DbCommand)(new SqlCommand(null, this));
        }

        public new SqlCommand CreateCommand()
        {
            return new SqlCommand(null, this); // doesn't associate a transaction, even if a transaction has begun
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new DriverException("SqlConnection: BeginDbTransaction method is not supported.");
        }

        public new SqlTransaction BeginTransaction()
        {
            return this.BeginTransaction("dummyName");
        }

        public SqlTransaction BeginTransaction(string transactionName)
        {
            SqlTransaction tran = new SqlTransaction(this);

            tran.BeginTran();

            return tran;
        }

        protected override void Dispose(bool disposing)
        {
            // note: public void Dispose() is already implemented in base class System.ComponentModel.Component
            //           which calls the overrriding Dispose(true) method implemented in this Sqlconnection subclass.
            // note: a closed socket cannot be reopen.

            if (this._disposed)
                return; 

            if (disposing)
            {
                this.Close(); // Close does the real job
            }

            base.Dispose(disposing); // dispose DBConnection and Component
            this._disposed = true;
        }

        public override void Close() // Microsoft docs says that Close and Dispose are functionally equivalent
        {
            if (this._disposed)
                return;

            this._connString = "";

            if (this._tcpClient != null)
            {
                this._tcpClient.Dispose(); // TcpClient.Close() just calls TcpClient.Dispose(). TcpClient.Dispose() closes the internal socket too.
                this._tcpClient = null;
            }
            this._socket = null;

            this._sendLock = null;
            this._buffout = null;
            this._buffin = null;
            this._state = ConnectionState.Broken;

            if (this._timer != null)
            {
                this._timer.Stop();
                this._timer.Close(); // Close() calls Dispose()
                this._timer = null;
            }

            this._transaction = null;
        }

        //===== helper methods =====

        // send content of this.buffout into the socket.
        internal void SendBuffout()
        {
            lock (this._sendLock) // only one thread at a time can Send() on the socket
            {
                byte[] bytes = this._buffout.ToByteArray();

                this._socket.Send(bytes);
            }
        }

        // send content of this.buffout into the socket with a timeout in seconds.
        internal void SendBuffoutWithTimeout(int timeout)
        {
            lock (this._sendLock) // only one thread at a time can Send() on the socket
            {
                byte[] bytes = this._buffout.ToByteArray();

                this._socket.SendTimeout = timeout * 1000; // milliseconds
                this._socket.Send(bytes);
                this._socket.SendTimeout = 0; // restore infinite timeout
            }
        }

        // called from the timer handler.
        private static void SendKeepAlive(SqlConnection sqlconn)
        {
            lock (sqlconn._sendLock) // only one thread at a time can Send() on the socket
            {
                try
                {
                    sqlconn._socket.Send(SqlConnection._keepaliveByte);
                    sqlconn._timer.Enabled = true;
                }
                catch
                {
                    // we just want to silently swallow the exception if any
                }
            }
        }

        // called asynchronously from another thread.
        // See SqlCommand Cancel() method.
        internal void SendCancel()
        {
            lock (this._sendLock) // only one thread at a time can Send() on the socket
            {
                try
                {
                    this._socket.Send(SqlConnection._cancelByte);
                }
                catch
                {
                    // we just want to silently swallow the exception if any
                }
            }
        }

        private void eatConnectionString(string connectionString)
        {
            // connection string must contain at least one attr=val pair

            if (connectionString.IndexOf('=') < 0)
                throw new DriverException("SqlConnection: connection string must contain attr=val pairs separated by semicolon.");

            // split connection string

            this._connString = connectionString;

            ConnStringAttributes attributes = new ConnStringAttributes(connectionString);
            this._serverAddr = attributes.serverAddr;
            this._serverPort = attributes.serverPort;
            this._login = attributes.login;
            this._password = attributes.password;
            this._database = attributes.database;
            this._connectionTimeout = attributes.connectionTimeout;
        }


    }
}
