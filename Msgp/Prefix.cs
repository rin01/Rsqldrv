
namespace Rsqldrv.Msgp
{
    static class Prefix
    {
        internal const byte
            M_NIL                  = 0xc0,
            M_FALSE                = 0xc2,
            M_TRUE                 = 0xc3,
            M_UINT8                = 0xcc,
            M_UINT16               = 0xcd,
            M_UINT32               = 0xce,
            M_UINT64               = 0xcf,
            M_INT8                 = 0xd0,
            M_INT16                = 0xd1,
            M_INT32                = 0xd2,
            M_INT64                = 0xd3,
            M_FLOAT32              = 0xca,
            M_FLOAT64              = 0xcb,
            M_FIXSTR_BASE          = 0xa0, // 3 MSB bits are significant
            M_STR8                 = 0xd9,
            M_STR16                = 0xda,
            M_STR32                = 0xdb,
            M_BIN8                 = 0xc4,
            M_BIN16                = 0xc5,
            M_BIN32                = 0xc6,
            M_FIXARRAY_BASE        = 0x90,// 4 MSB bits are significant
            M_ARRAY16              = 0xdc,
            M_ARRAY32              = 0xdd,
            M_FIXMAP_BASE          = 0x80, // 4 MSB bits are significant
            M_MAP16                = 0xde,
            M_MAP32                = 0xdf,
            M_NEGATIVE_FIXINT_BASE = 0xe0, // 11100000 to 11111111 are negative fixint numbers

            PREFIX_FIXSTR_MASK     = 0xe0, // 11100000
            PREFIX_FIXARRAY_MASK   = 0xf0, // 11110000
            PREFIX_FIXMAP_MASK     = 0xf0; // 11110000
    }
}
