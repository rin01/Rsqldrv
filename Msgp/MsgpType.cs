
namespace Rsqldrv.Msgp
{
    // MessagePack can encode these kinds of datatypes.
    // When a value is encoded, the first byte will contain a more precise type of the value.
    //    So, for a UintegerType value, the first byte will be Prefix.M_UINT8, Prefix.M_UINT16 etc, depending on whether the value is small or big.
    enum MsgpType : byte
    {
        InvalidType = 0,

        NilType, // for SQL NULL value
        BoolType,
        UintegerType, // any unsigned integer, 8, 16, 32, or 64 bits
        IntegerType,  // any signed integer,   8, 16, 32, or 64 bits
        Float32Type,  // never used in rsql protocol
        Float64Type,

        BinType, // binary string (array of bytes)
        StrType, // string encoded in utf8

        ArrayType, // array of n values, which can be of different types. The name ArrayType is misleading, as it is in fact more a structure.
        MapType,   // map of n <key, value> pairs, which can be of different types.
    };
}
