// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Microsoft.Metadata.Tools;

internal sealed class TableBuilder
{
    private readonly string _title;
    private readonly string[] _header;
    private readonly List<(string[] fields, string details)> _rows;

    public char HorizontalSeparatorChar = '=';
    public string Indent = "";
    public int FirstRowNumber = 1;

    public TableBuilder(string title, params string[] header)
    {
        _rows = new List<(string[] fields, string details)>();
        _title = title;
        _header = header;
    }

    public int RowCount
        => _rows.Count;

    public void AddRow(params string[] fields)
        => AddRowWithDetails(fields, details: null);

    public void AddRowWithDetails(string[] fields, string details)
    {
        Debug.Assert(_header.Length == fields.Length);
        _rows.Add((fields, details));
    }

    public void WriteTo(TextWriter writer)
    {
        if (_rows.Count == 0)
        {
            return;
        }

        if (_title != null)
        {
            writer.Write(Indent);
            writer.WriteLine(_title);
        }

        string columnSeparator = "  ";
        var columnWidths = new int[_rows.First().fields.Length];

        void updateColumnWidths(string[] fields)
        {
            for (int i = 0; i < fields.Length; i++)
            {
                columnWidths[i] = Math.Max(columnWidths[i], fields[i].Length + columnSeparator.Length);
            }
        }

        updateColumnWidths(_header);

        foreach (var (fields, _) in _rows)
        {
            updateColumnWidths(fields);
        }

        void writeRow(string[] fields)
        {
            for (int i = 0; i < fields.Length; i++)
            {
                var field = fields[i];

                writer.Write(field);
                writer.Write(new string(' ', columnWidths[i] - field.Length));
            }
        }

        // header:
        int rowNumberWidth = (FirstRowNumber + _rows.Count - 1).ToString("x").Length;
        int tableWidth = Math.Max(_title?.Length ?? 0, columnWidths.Sum() + columnWidths.Length);
        string horizontalSeparator = new string(HorizontalSeparatorChar, tableWidth);

        writer.Write(Indent);
        writer.WriteLine(horizontalSeparator);

        writer.Write(Indent);
        writer.Write(new string(' ', rowNumberWidth + 2));

        writeRow(_header);

        writer.WriteLine();
        writer.Write(Indent);
        writer.WriteLine(horizontalSeparator);

        // rows:
        int rowNumber = FirstRowNumber;
        foreach (var (fields, details) in _rows)
        {
            string rowNumberStr = rowNumber.ToString("x");
            writer.Write(Indent);
            writer.Write(new string(' ', rowNumberWidth - rowNumberStr.Length));
            writer.Write(rowNumberStr);
            writer.Write(": ");

            writeRow(fields);
            writer.WriteLine();

            if (details != null)
            {
                writer.Write(Indent);
                writer.Write(details);
            }

            rowNumber++;
        }

        writer.WriteLine();
    }
}
