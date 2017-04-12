using System;

namespace Rsqldrv.SqlClient
{
    // status is the internal state of execution of the batch.
    static class BatchStatus
    {
        internal const byte BATCH_SENT              = 1; // SQL text has been sent to the server
        internal const byte RECORD_LAYOUT_AVAILABLE = 2; // set when recordset is detected, returning control to the caller
        internal const byte RECORD_DATA_AVAILABLE   = 3; // a record is available for read
        internal const byte RECORD_END              = 4; // no more record in recordset
        internal const byte BATCH_END               = 5; // batch has terminated (successfully or because of an error)
    }
}
