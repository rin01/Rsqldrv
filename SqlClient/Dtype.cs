
namespace Rsqldrv.SqlClient
{
    // when the driver receives column datatypes from the socket, they are represented by these constants
    static class Dtype
    {
        internal const byte VOID = 1;
        internal const byte BOOLEAN = 2;
        internal const byte VARBINARY = 4;
        internal const byte VARCHAR = 6;

        internal const byte BIT = 9;
        internal const byte TINYINT = 10;
        internal const byte SMALLINT = 11;
        internal const byte INT = 12;
        internal const byte BIGINT = 13;

        internal const byte MONEY = 15;
        internal const byte NUMERIC = 16;
        internal const byte FLOAT = 17;

        internal const byte DATE = 19;
        internal const byte TIME = 20;
        internal const byte DATETIME = 21;
    }
}


