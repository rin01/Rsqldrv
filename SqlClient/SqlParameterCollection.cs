using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;

namespace Rsqldrv.SqlClient
{
    public sealed class SqlParameterCollection : DbParameterCollection
    {
        private static readonly int PARAM_LIST_INITIAL_CAPACITY = 10;

        private List<SqlParameter> _paramlist = new List<SqlParameter>(PARAM_LIST_INITIAL_CAPACITY); // never null

        //===== constructors =====

        internal SqlParameterCollection() {}

        //===== API properties =====

        public override int Count { get { return this._paramlist.Count; } }

        public override object SyncRoot { get { throw new DriverException("SqlParameterCollection: SyncRoot property is not supported."); } }

        public new SqlParameter this[int index]
        {
            get { return this._paramlist[index]; }
            set { this._paramlist[index] = value; }
        }

        public new SqlParameter this[string parameterName]
        {
            get { int index = this.IndexOf(parameterName); return this._paramlist[index]; }
            set { int index = this.IndexOf(parameterName); this._paramlist[index] = value; }
        }

        //===== API methods =====

        public override int Add(object value)
        {
            this.Add((SqlParameter)value);
            return this._paramlist.Count - 1;
        }

        public SqlParameter Add(SqlParameter value)
        {
            if (value == null)
                throw new ArgumentNullException("SqlParameterCollection: the SqlParameter to add cannot be null.");

            if (this._paramlist.Contains(value))
                throw new ArgumentException("SqlParameterCollection: the SqlParameter to add already exists in SqlParameterCollection.");

            this._paramlist.Add(value);
            return null; // doc says it returns a new SqlParameter, but in fact, it returns the parameter passed as argument.
        }

        public SqlParameter Add(string parameterName, SqlDbType sqlDbType)
        {
            SqlParameter param = new SqlParameter(parameterName, sqlDbType);
            this.Add(param);
            return param;
        }

        public SqlParameter Add(string parameterName, SqlDbType sqlDbType, int size)
        {
            SqlParameter param = new SqlParameter(parameterName, sqlDbType, size);
            this.Add(param);
            return param;
        }

        public override void AddRange(Array values)
        {
            foreach (object param in values)
                this._paramlist.Add((SqlParameter)param);
        }

        public void AddRange(SqlParameter[] values)
        {
            foreach (SqlParameter param in values)
                this._paramlist.Add(param);
        }

        public override void Clear()
        {
            this._paramlist.Clear();
        }

        public override bool Contains(object value)
        {
            return this._paramlist.Contains((SqlParameter)value);
        }

        public bool Contains(SqlParameter value)
        {
            return this._paramlist.Contains(value);
        }

        public override bool Contains(string parameterName)
        {
            int index = this.IndexOf(parameterName);
            if (index >= 0)
                return true;

            return false;
        }

        public override void CopyTo(Array array, int index)
        {
            throw new DriverException("SqlParameterCollection: method CopyTo is not supported.");
        }

        public override System.Collections.IEnumerator GetEnumerator()
        {
            return this._paramlist.GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            return this._paramlist.IndexOf((SqlParameter)value);
        }

        public int IndexOf(SqlParameter value)
        {
            return this._paramlist.IndexOf(value);
        }

        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < this._paramlist.Count; i++)
            {
                if (this._paramlist[i].ParameterName == parameterName)
                    return i;
            }

            return -1;
        }

        public override void Insert(int index, object value)
        {
            this._paramlist.Insert(index, (SqlParameter)value);
        }

        public void Insert(int index, SqlParameter value)
        {
            this._paramlist.Insert(index, value);
        }

        public override void Remove(object value)
        {
            this._paramlist.Remove((SqlParameter)value);
        }

        public void Remove(SqlParameter value)
        {
            this._paramlist.Remove(value);
        }

        public override void RemoveAt(int index)
        {
            this._paramlist.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            int index = this.IndexOf(parameterName);
            this._paramlist.RemoveAt(index);
        }

        internal string GetAllDECLAREstring()
        {
            if (this._paramlist.Count == 0)
                return "";

            var sb = new StringBuilder(this._paramlist.Count);

            foreach (SqlParameter param in this._paramlist)
            {
                sb.AppendLine(param.GetDECLAREstring());
            }

            sb.AppendLine();
            return sb.ToString();
        }

        //===== protected overriding methods =====

        protected override void SetParameter(int index, DbParameter value)
        {
            this._paramlist[index] = (SqlParameter)value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = this.IndexOf(parameterName);
            this._paramlist[index] = (SqlParameter)value;
        }

        protected override DbParameter GetParameter(int index)
        {
            return this._paramlist[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            int index = this.IndexOf(parameterName);
            return this._paramlist[index];
        }
    }
}
