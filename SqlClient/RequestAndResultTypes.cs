using System;
using System.Collections.Generic;

namespace Rsqldrv.SqlClient
{
    // response codes received from the server.
    static class ResType
    {
        internal const uint LOGIN_FAILED  = 0;
        internal const uint LOGIN_SUCCESS = 1;

        internal const uint RECORD_LAYOUT = 3;
        internal const uint RECORD_DATA = 4;
        internal const uint RECORD_FINISHED = 5;

        internal const uint EXECUTION_FINISHED = 7;

        internal const uint PRINT = 10;
        internal const uint MESSAGE = 11;
        internal const uint ERROR = 12;

        internal const uint BATCH_END = 14;
    }

    // request codes sent by the driver to the server.
    static class ReqType
    {
        internal const uint AUTH = 20;      // authentication message
        internal const uint BATCH = 21;     // batch text
        internal const uint KEEPALIVE = 30; // keepalive message
        internal const uint CANCEL = 100;   // cancel message
    }
}
