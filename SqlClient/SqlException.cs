using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Rsqldrv.SqlClient
{
    public class SqlException : DbException
    {
        private SqlErrorCollection _errColl = new SqlErrorCollection();

        //===== constructors =====

        public SqlException(SqlError sqlError) : base(sqlError.Message)
        {
            this._errColl.Add(sqlError);
        }

        //===== properties =====

        public SqlErrorCollection Errors { get { return this._errColl; } }

        public byte Class { get { return this._errColl[0].Class; } }

        public int LineNumber { get { return this._errColl[0].LineNumber; } }

        // public override string Message { get { return this._errColl[0].Message; } }  // not needed, as message has been copied in base class by constructor

        public int Number { get { return this._errColl[0].Number; } } // N/A

        public string Procedure { get { return this._errColl[0].Procedure; } }

        public string Server { get { return this._errColl[0].Server; } }

        public override string Source { get { return this._errColl[0].Source; } }

        public byte State { get { return this._errColl[0].State; } }

        public override string ToString()
        {
            return this._errColl[0].ToString();
        }

    }
}
