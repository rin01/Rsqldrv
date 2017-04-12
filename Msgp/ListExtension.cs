using System;
using System.Collections.Generic;

namespace Rsqldrv.Msgp
{
    static class ListExtension
    {
        // extension method that allows to add many items into a List<T>.
        //     example: mylist.AddMany(a, b, c);
        internal static void AddMany<T>(this List<T> list, params T[] elements)
        {
            list.AddRange(elements);
        }
    }
}
