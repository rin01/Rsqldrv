using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Rsqldrv.SqlClient
{
    public sealed class SqlErrorCollection : System.Collections.ICollection, System.Collections.IEnumerable
    {
        private List<SqlError> _coll = new List<SqlError>(1);

        //===== properties ======

        public int Count { get { return this._coll.Count; } }

        public SqlError this[int index] { get { return this._coll[index]; } }

        //===== methods ======

        public IEnumerator GetEnumerator()
        {
            return this._coll.GetEnumerator();
        }

        internal void Add(SqlError sqlError)
        {
            this._coll.Add(sqlError);
        }

        bool System.Collections.ICollection.IsSynchronized { get { return false; } }

        object System.Collections.ICollection.SyncRoot { get { throw new DriverException("SqlErrorCollection: SyncRoot property is not supported."); } }

        public void CopyTo(Array array, int index)
        {
            throw new DriverException("SqlErrorCollection: method CopyTo is not supported.");
        }    
    }
}
