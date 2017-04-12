using System;
using System.Collections.Generic;

namespace Rsqldrv.SqlClient
{
    public class DriverException : System.Exception
    {
        public DriverException() : base() { }
        public DriverException(string message) : base(message) { }
        public DriverException(string message, System.Exception inner) : base(message, inner) { }
    }
}
