// Licensed to the.NET Foundation under one or more agreements.
// The.NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;

namespace Microsoft.Metadata.Tools;

internal static class MetadataReaderExtensions
{
    public static int GetTableSize(this MetadataReader reader, TableIndex table)
        => reader.GetTableRowCount(table) * reader.GetTableRowSize(table);
}
