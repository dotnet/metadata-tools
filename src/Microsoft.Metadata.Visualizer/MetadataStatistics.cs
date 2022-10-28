// Licensed to the.NET Foundation under one or more agreements.
// The.NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

using System;
using System.Collections.Immutable;
using System.IO;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;

namespace Microsoft.Metadata.Tools;

public sealed class MetadataStatistics
{
    private readonly MetadataReader _reader;
    private readonly ImmutableDictionary<BlobHandle, BlobKind> _blobKinds;
    private readonly TextWriter _writer;

    public MetadataStatistics(TextWriter writer, MetadataReader reader, ImmutableDictionary<BlobHandle, BlobKind> blobKinds)
    {
        _writer = writer;
        _reader = reader;
        _blobKinds = blobKinds;
    }

    public void Visualize()
    {
        WriteTableAndHeapSizes();
        WriteBlobSizes();
    }

    internal void WriteTableAndHeapSizes()
    {
        var table = new TableBuilder("Table and Heap sizes",
            "Table/Heap",
            "Size [B]",
            "% of metadata");

        double totalSize = _reader.MetadataLength;

        foreach (TableIndex index in Enum.GetValues(typeof(TableIndex)))
        {
            var size = _reader.GetTableSize(index);
            if (size != 0)
            {
                table.AddRow(index.ToString(), $"{size,10}", $"{100 * size / totalSize,5:F2}%");
            }
        }

        foreach (HeapIndex index in Enum.GetValues(typeof(HeapIndex)))
        {
            var size = _reader.GetHeapSize(index);
            if (size != 0)
            {
                table.AddRow($"#{index}", $"{size,10}", $"{100 * size / totalSize,5:F2}%");
            }
        }

        table.WriteTo(_writer);
    }

    internal void WriteBlobSizes()
    {
        var table = new TableBuilder("Blob sizes",
           "Kind",
           "Size [B]",
           "% of #Blob",
           "% of metadata");

        var allKinds = (BlobKind[])Enum.GetValues(typeof(BlobKind));
        var sizePerKind = new int[allKinds.Length];
        foreach (var entry in _blobKinds)
        {
            var handle = entry.Key;
            var kind = entry.Value;

            sizePerKind[(int)kind] += _reader.GetBlobReader(handle).Length;
        }

        var sum = 0;
        double totalMetadataSize = _reader.MetadataLength;
        double totalBlobSize = _reader.GetHeapSize(HeapIndex.Blob);
        for (int i = 0; i < sizePerKind.Length; i++)
        {
            var size = sizePerKind[i];
            if (size > 0)
            {
                table.AddRow($"{(BlobKind)i}", $"{size,10}", $"{100 * size / totalBlobSize,5:F2}%", $"{100 * size / totalMetadataSize,5:F2}%");
                sum += size;
            }
        }

        var miscSize = totalBlobSize - sum;
        table.AddRow("<misc>", $"{miscSize,10}", $"{100 * miscSize / totalBlobSize,5:F2}%", $"{100 * miscSize / totalMetadataSize,5:F2}%");

        table.WriteTo(_writer);
    }
}