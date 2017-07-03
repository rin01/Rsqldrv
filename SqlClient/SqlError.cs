using System;
using System.Text;
using Rsqldrv.Msgp;

namespace Rsqldrv.SqlClient
{
    public sealed class SqlError
    {
        private string _srcFile = "";
        private long _srcLineNo;
        private string _srcFuncname = "";
        private string _srcBacktrace = "";

        private string _category = "";
        private string _message = "";
        private string _severity = "";
        private long _state;
        private string _text = "";
        private long _lineNo;
        private long _linePos;

        private string _server = "";

        //===== constructor =====

        internal SqlError(string serverName)
        {
            this._server = serverName;
        }

        //===== API properties =====

        public byte Class // severity of the error
        {
            get
            {
                if (this._state == 127)
                    return 20;

                if (this._severity.EndsWith("WARNING"))
                    return 1; // warnings must be ignored by the driver
                
                return 16; // ordinary error
            }
        }

        public int LineNumber { get { return (int)this._lineNo; } } // N/A

        public string Message { get { return String.Format("{0}:{1} {2} (state={3})", this._lineNo, this._linePos, this._text, this._state); } }

        public int Number { get { return -1; } } // N/A

        public string Procedure { get { return "<N/A>"; } }

        public string Server { get { return this._server; } }

        public string Source { get { return "RSQL driver"; } }

        public byte State { get { return (byte)this._state; } } // default 1. If 127, session is aborted.

        //===== non-standard API properties =====

        public string _SrcFile { get { return this._srcFile; } }

        public long _SrcLineNo { get { return this._srcLineNo; } }

        public string _SrcFuncname { get { return this._srcFuncname; } }

        public string _ServerBacktrace { get { return this._srcBacktrace; } }

        public string _Category { get { return this._category; } }

        public string _Label { get { return this._message; } } // instead of Message, which is already used to return this._text

        public string _Severity { get { return this._severity; } }

        public long _LinePosition { get { return this._linePos; } }

        public override string ToString()
        {
            return String.Format("{0}:{1} {2} (state={3})", this._lineNo, this._linePos, this._text, this._state);
        }

        //===== helper function to read error data from socket =====

        internal static SqlError ReadSqlError(BufferIn buffin, string serverName)
        {
            SqlError sqlError = new SqlError(serverName);

            // read fields of error message

            int errObjSize = buffin.ReadMapHeader();

            for (int i = 0; i < errObjSize; i++)
            {
                string key = buffin.ReadString();

                switch (key)
                {
                    case "src_file":
                        sqlError._srcFile = buffin.ReadString();
                        break;
                    case "src_line_no":
                        sqlError._srcLineNo = buffin.ReadLong();
                        break;
                    case "src_funcname":
                        sqlError._srcFuncname = buffin.ReadString();
                        break;
                    case "src_backtrace":
                        sqlError._srcBacktrace = buffin.ReadString();
                        break;

                    case "category":
                        sqlError._category = buffin.ReadString();
                        break;
                    case "message":
                        sqlError._message = buffin.ReadString();
                        break;
                    case "severity":
                        sqlError._severity = buffin.ReadString();
                        break;
                    case "state":
                        sqlError._state = buffin.ReadLong();
                        break;
                    case "text":
                        sqlError._text = buffin.ReadString();
                        break;
                    case "line_no":
                        sqlError._lineNo = buffin.ReadLong();
                        break;
                    case "line_pos":
                        sqlError._linePos = buffin.ReadLong();
                        break;

                    default:
                        throw new DriverException(String.Format("SqlError: unknown error key \"{0}\" has been received.", key));
                }
            }

            return sqlError;
        }

    }
}
