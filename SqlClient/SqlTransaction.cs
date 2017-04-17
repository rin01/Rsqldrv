using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Transactions;

namespace Rsqldrv.SqlClient
{
    public sealed class SqlTransaction : DbTransaction
    {
        private SqlConnection _conn;

        private bool _disposed;

        //===== constructor =====

        internal SqlTransaction(SqlConnection conn)
        {
            this._conn = conn;
        }

        //===== API properties =====

        public new SqlConnection Connection { get { return this._conn; } }

        public override System.Data.IsolationLevel IsolationLevel { get { return System.Data.IsolationLevel.ReadCommitted; } }

        //===== API methods =====

        internal void BeginTran()
        {
            this.executeSingleCommand("BEGIN TRAN;");
            this._conn._transaction = this;
        }

        public override void Commit()
        {
            this.executeSingleCommand("COMMIT;");
            this._conn._transaction = null;
        }

        public override void Rollback()
        {
            this.executeSingleCommand("IF @@TRANCOUNT > 0 ROLLBACK;");
            this._conn._transaction = null;
        }

        //===== some overriding methods =====

        protected override DbConnection DbConnection { get { return (DbConnection)this._conn; } }

        protected override void Dispose(bool disposing)
        {
            if (this._disposed)
                return;

            if (disposing)
            {
                this._conn = null;
            }

            // base.Dispose(disposing); // dispose DbCommand. Useless, base.Dispose(bool) does nothing.
            this._disposed = true;
        }

        //===== helper function =====

        internal void executeSingleCommand(string text)
        {
            SqlDataReader reader = null;

            try
            {
                reader = new SqlDataReader(text, this._conn, CommandBehavior.Default);
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
        }

    }
}
