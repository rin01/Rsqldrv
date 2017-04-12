using System;
using System.Collections.Generic;

namespace Rsqldrv
{
    // exception raised when an assertion fails in the driver code.
    public class RsqlAssertFailedException : System.Exception
    {
        public RsqlAssertFailedException() : base() { }
        public RsqlAssertFailedException(string message) : base(message) { }
        public RsqlAssertFailedException(string message, System.Exception inner) : base(message, inner) { }
    }

    class Assert
    {
        internal static void check(bool b)
        {
            if (b == false)
                throw new RsqlAssertFailedException("assertion failed");
        }
    }
}
