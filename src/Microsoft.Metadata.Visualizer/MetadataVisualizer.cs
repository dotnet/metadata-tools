// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

#nullable disable

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using Microsoft.CodeAnalysis.Debugging;
using Roslyn.Utilities;

namespace Microsoft.Metadata.Tools
{
    [Flags]
    public enum MetadataVisualizerOptions
    {
        None = 0,
        ShortenBlobs = 1,
        NoHeapReferences = 1 << 1,
        EmbeddedSource = 1 << 2
    }

    public sealed partial class MetadataVisualizer
    {
        private const string BadMetadataStr = "<bad metadata>";

        [Flags]
        private enum BlobKind
        {
            None = 0,
            Key = 1 << 0,
            FileHash = 1 << 1,

            MethodSignature = 1 << 2,
            FieldSignature = 1 << 3,
            MemberRefSignature = 1 << 4,
            StandAloneSignature = 1 << 5,

            TypeSpec = 1 << 6,
            MethodSpec = 1 << 7,

            ConstantValue = 1 << 8,
            Marshalling = 1 << 9,
            PermissionSet = 1 << 10,
            CustomAttribute = 1 << 11,

            DocumentName = 1 << 12,
            DocumentHash = 1 << 13,
            SequencePoints = 1 << 14,
            Imports = 1 << 15,
            ImportAlias = 1 << 16,
            ImportNamespace = 1 << 17,
            LocalConstantSignature = 1 << 18,
            CustomDebugInformation = 1 << 19,
        }

        [Flags]
        private enum StringKind
        {
            None = 0,
            TypeName = 1 << 0,
            MethodName = 1 << 1,
            FieldName = 1 << 2,
            NamespaceName = 1 << 3,
            ModuleName = 1 << 4,
            MethodImportName = 1 << 5,
            ParamName = 1 << 6,
            MemberName = 1 << 7,
            EventName = 1 << 8,
            ResourceName = 1 << 9,
            PropertyName = 1 << 10,
            AssemblyName = 1 << 11,
            CultureName = 1 << 12,
            GenericParamName = 1 << 13,
            LocalVariableName = 1 << 14,
            ConstantName = 1 << 15,
            FileName = 1 << 16
        }

        private delegate TResult FuncRef<TArg, TResult>(ref TArg arg); 

        private sealed class TableBuilder
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

                void updateColumnWidths( string[] fields)
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
            }
        }

        private readonly TextWriter _writer;
        private readonly IReadOnlyList<MetadataReader> _readers;
        private readonly MetadataAggregator _aggregator;
        private readonly MetadataVisualizerOptions _options;
        private readonly SignatureVisualizer _signatureVisualizer;

        // enc map for each delta reader
        private readonly ImmutableArray<ImmutableArray<EntityHandle>> _encMaps;
        private readonly ImmutableDictionary<EntityHandle, EntityHandle> _encAddedMemberToParentMap;

        private int _stringHeapBaseOffset = 0;
        private int _blobHeapBaseOffset = 0;
        private int _generation = -1;
        private MetadataReader _reader;
        private readonly Dictionary<BlobHandle, BlobKind> _blobKinds = [];
        private readonly Dictionary<StringHandle, StringKind> _stringKinds = [];

        private MetadataVisualizer(TextWriter writer, IReadOnlyList<MetadataReader> readers, MetadataVisualizerOptions options = MetadataVisualizerOptions.None)
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
            _readers = readers ?? throw new ArgumentNullException(nameof(readers));
            _options = options;
            _signatureVisualizer = new SignatureVisualizer(this);

            if (readers.Count > 1)
            {
                var deltaReaders = new List<MetadataReader>(readers.Skip(1));
                _aggregator = new MetadataAggregator(readers[0], deltaReaders);

                _encMaps = ImmutableArray.CreateRange(deltaReaders.Select(reader => ImmutableArray.CreateRange(reader.GetEditAndContinueMapEntries())));
                _encAddedMemberToParentMap = CalculateEncAddedMemberToParentMap();
            }
            else
            {
                _encMaps = ImmutableArray<ImmutableArray<EntityHandle>>.Empty;
                _encAddedMemberToParentMap = ImmutableDictionary<EntityHandle, EntityHandle>.Empty;
            }
        }

        public MetadataVisualizer(MetadataReader reader, TextWriter writer, MetadataVisualizerOptions options = MetadataVisualizerOptions.None)
            : this(writer, new[] { reader ?? throw new ArgumentNullException(nameof(reader)) }, options)
        {
            _reader = reader;
        }

        public MetadataVisualizer(IReadOnlyList<MetadataReader> readers, TextWriter writer, MetadataVisualizerOptions options = MetadataVisualizerOptions.None)
            : this(writer, readers, options)
        {
        }

        private ImmutableDictionary<EntityHandle, EntityHandle> CalculateEncAddedMemberToParentMap()
        {
            var builder = ImmutableDictionary.CreateBuilder<EntityHandle, EntityHandle>();

            foreach (var reader in _readers)
            {
                var currentParent = default(EntityHandle);

                foreach (var entry in reader.GetEditAndContinueLogEntries())
                {
                    switch (entry.Operation)
                    {
                        case EditAndContinueOperation.AddMethod:
                        case EditAndContinueOperation.AddProperty:
                        case EditAndContinueOperation.AddEvent:
                        case EditAndContinueOperation.AddField:
                        case EditAndContinueOperation.AddParameter:
                            Debug.Assert(currentParent.IsNil);
                            currentParent = entry.Handle;
                            break;

                        case EditAndContinueOperation.Default:
                            if (!currentParent.IsNil)
                            {
                                builder.Add(entry.Handle, currentParent);
                                currentParent = default;
                            }

                            break;
                    }
                }
            }

            return builder.ToImmutable();
        }

        private bool IsDelta => _reader.GetTableRowCount(TableIndex.EncLog) > 0;

        private bool NoHeapReferences => (_options & MetadataVisualizerOptions.NoHeapReferences) != 0;

        public void VisualizeAllGenerations()
        {
            for (int i = 0; i < _readers.Count; i++)
            {
                _writer.WriteLine(">>>");
                _writer.WriteLine($">>> Generation {i}:");
                _writer.WriteLine(">>>");
                _writer.WriteLine();

                Visualize(i);
            }
        }

        public void Visualize(int generation = -1)
        {
            if (generation < 0)
            {
                generation = _readers.Count - 1;
            }

            _generation = generation;
            _reader = _readers[generation];

            var tables = ReadTables();

            if (IsDelta && _aggregator == null)
            {
                _stringHeapBaseOffset = GetHeapBaseOffset(generation, HeapIndex.String, _stringKinds.Keys, MetadataTokens.GetHeapOffset);
                _blobHeapBaseOffset = GetHeapBaseOffset(generation, HeapIndex.Blob, _blobKinds.Keys, MetadataTokens.GetHeapOffset);

                // read the tables again, this time we use the base offset:
                tables = ReadTables();
            }

            foreach (var table in tables)
            {
                WriteTable(table);
            }

            // heaps:
            WriteUserStrings();
            WriteStrings(generation);
            WriteBlobs(generation);
            WriteGuids();

            WriteCustomAttributeSizes();

            IList<TableBuilder> ReadTables() =>
            [
                // type-system tables:
                ReadModuleTable(),
                ReadTypeRefTable(),
                ReadTypeDefTable(generation),
                ReadFieldTable(generation),
                ReadMethodTable(generation),
                ReadParamTable(generation),
                ReadMemberRefTable(),
                ReadConstantTable(),
                ReadCustomAttributeTable(),
                ReadDeclSecurityTable(),
                ReadStandAloneSigTable(),
                ReadEventTable(),
                ReadPropertyTable(),
                ReadMethodImplTable(),
                ReadModuleRefTable(),
                ReadTypeSpecTable(),
                ReadEnCLogTable(),
                ReadEnCMapTable(),
                ReadAssemblyTable(),
                ReadAssemblyRefTable(),
                ReadFileTable(),
                ReadExportedTypeTable(),
                ReadManifestResourceTable(),
                ReadGenericParamTable(generation),
                ReadMethodSpecTable(),
                ReadGenericParamConstraintTable(),

                // debug tables:
                ReadDocumentTable(),
                ReadMethodDebugInformationTable(),
                ReadLocalScopeTable(),
                ReadLocalVariableTable(),
                ReadLocalConstantTable(),
                ReadImportScopeTable(),
                ReadCustomDebugInformationTable()
            ];
        }

        private string MakeTableName(TableIndex index)
            => $"{index} (index: 0x{(byte)index:X2}, size: {_reader.GetTableRowCount(index) * _reader.GetTableRowSize(index)}): ";

        private void WriteTable(TableBuilder table)
        {
            if (table.RowCount > 0)
            {
                table.WriteTo(_writer);
                _writer.WriteLine();
            }
        }

        private EntityHandle GetAggregateHandle(EntityHandle generationHandle, int generation)
        {
            if (generation == 0)
            {
                return generationHandle;
            }

            var encMap = _encMaps[generation - 1];

            if (!TryGetHandleRange(encMap, generationHandle.Kind, out int start, out _))
            {
                throw new BadImageFormatException(string.Format("EncMap is missing record for {0:8X}.", MetadataTokens.GetToken(generationHandle)));
            }

            return encMap[start + MetadataTokens.GetRowNumber(generationHandle) - 1];
        }

        private static bool TryGetHandleRange(ImmutableArray<EntityHandle> handles, HandleKind handleType, out int start, out int count)
        {
            TableIndex tableIndex;
            MetadataTokens.TryGetTableIndex(handleType, out tableIndex);

            int mapIndex = handles.BinarySearch(MetadataTokens.Handle(tableIndex, 0), TokenTypeComparer.Instance);
            if (mapIndex < 0)
            {
                start = 0;
                count = 0;
                return false;
            }

            int s = mapIndex;
            while (s >= 0 && handles[s].Kind == handleType)
            {
                s--;
            }

            int e = mapIndex;
            while (e < handles.Length && handles[e].Kind == handleType)
            {
                e++;
            }

            start = s + 1;
            count = e - start;
            return true;
        }

        private BlobReader GetGenerationBlobReader(BlobHandle handle)
           => GetGenerationEntity(handle, (reader, handle) => reader.GetBlobReader((BlobHandle)handle));

        private TypeDefinition GetGenerationTypeDefinition(TypeDefinitionHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetTypeDefinition((TypeDefinitionHandle)handle));

        private MethodDefinition GetGenerationMethodDefinition(MethodDefinitionHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetMethodDefinition((MethodDefinitionHandle)handle));

        private MemberReference GetGenerationMemberReference(MemberReferenceHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetMemberReference((MemberReferenceHandle)handle));

        private TypeReference GetGenerationTypeDefinition(TypeReferenceHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetTypeReference((TypeReferenceHandle)handle));

        private MethodSpecification GetGenerationMethodSpecification(MethodSpecificationHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetMethodSpecification((MethodSpecificationHandle)handle));

        private TypeSpecification GetGenerationTypeSpecification(TypeSpecificationHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetTypeSpecification((TypeSpecificationHandle)handle));

        private StandaloneSignature GetGenerationLocalSignature(StandaloneSignatureHandle handle)
            => GetGenerationEntity(handle, (reader, handle) => reader.GetStandaloneSignature((StandaloneSignatureHandle)handle));

        /// <summary>
        /// Returns entity definition that can be used to read its metadata.
        /// The entity is local to the generation that introduced it and can't be used to look up related entities such as declaring type, etc.
        /// since the metadata tables contain aggregate metadata tokens and not generation-relative ones, which is stored in the returned entity.
        /// </summary>
        private TEntity GetGenerationEntity<TEntity>(Handle handle, Func<MetadataReader, Handle, TEntity> getter)
        {
            if (_aggregator != null)
            {
                var generationHandle = _aggregator.GetGenerationHandle(handle, out int generation);
                return getter(_readers[generation], generationHandle);
            }
            else
            {
                return getter(_reader, handle);
            }
        }

        private static readonly Guid s_CSharpGuid = new Guid("3f5162f8-07c6-11d3-9053-00c04fa302a1");
        private static readonly Guid s_visualBasicGuid = new Guid("3a12d0b8-c26c-11d0-b442-00a0244a1dd2");
        private static readonly Guid s_FSharpGuid = new Guid("ab4f38c9-b6e6-43ba-be3b-58080b2ccce3");
        private static readonly Guid s_sha1Guid = new Guid("ff1816ec-aa5e-4d10-87f7-6f4963833460");
        private static readonly Guid s_sha256Guid = new Guid("8829d00f-11b8-4213-878b-770e8597ac16");

        private static string GetLanguage(Guid guid)
        {
            if (guid == s_CSharpGuid) return "C#";
            if (guid == s_visualBasicGuid) return "Visual Basic";
            if (guid == s_FSharpGuid) return "F#";

            return "{" + guid + "}";
        }

        private static string GetHashAlgorithm(Guid guid)
        {
            if (guid == s_sha1Guid) return "SHA-1";
            if (guid == s_sha256Guid) return "SHA-256";

            return "{" + guid + "}";
        }

        private string GetCustomDebugInformationKind(Guid guid)
        {
            if (guid == PortableCustomDebugInfoKinds.AsyncMethodSteppingInformationBlob) return "Async Method Stepping Information";
            if (guid == PortableCustomDebugInfoKinds.StateMachineHoistedLocalScopes) return "State Machine Hoisted Local Scopes";
            if (guid == PortableCustomDebugInfoKinds.DynamicLocalVariables) return "Dynamic Local Variables";
            if (guid == PortableCustomDebugInfoKinds.TupleElementNames) return "Tuple Element Names";
            if (guid == PortableCustomDebugInfoKinds.DefaultNamespace) return "Default Namespace";
            if (guid == PortableCustomDebugInfoKinds.EncLocalSlotMap) return "EnC Local Slot Map";
            if (guid == PortableCustomDebugInfoKinds.EncLambdaAndClosureMap) return "EnC Lambda and Closure Map";
            if (guid == PortableCustomDebugInfoKinds.EmbeddedSource) return "Embedded Source";
            if (guid == PortableCustomDebugInfoKinds.SourceLink) return "Source Link";
            if (guid == PortableCustomDebugInfoKinds.CompilationMetadataReferences) return "Compilation Metadata References";
            if (guid == PortableCustomDebugInfoKinds.CompilationOptions) return "Compilation Options";
            if (guid == PortableCustomDebugInfoKinds.TypeDefinitionDocuments) return "Type Definition Documents";
            if (guid == PortableCustomDebugInfoKinds.PrimaryConstructorInformationBlob) return "Primary Constructor";

            return "{" + guid + "}";
        }

        private string Language(Func<GuidHandle> getHandle) =>
            Literal(() => getHandle(), (r, h) => GetLanguage(r.GetGuid((GuidHandle)h)));

        private string HashAlgorithm(Func<GuidHandle> getHandle) =>
            Literal(() => getHandle(), (r, h) => GetHashAlgorithm(r.GetGuid((GuidHandle)h)));

        private string CustomDebugInformationKind(Func<GuidHandle> getHandle) =>
            Literal(() => getHandle(), (r, h) => GetCustomDebugInformationKind(r.GetGuid((GuidHandle)h)));

        private string DocumentName(Func<DocumentNameBlobHandle> getHandle) =>
            Literal(() => getHandle(), BlobKind.DocumentName, (r, h) => "'" + StringUtilities.EscapeNonPrintableCharacters(r.GetString((DocumentNameBlobHandle)h)) + "'");

        private string LiteralUtf8Blob(Func<BlobHandle> getHandle, BlobKind kind)
        {
            return Literal(getHandle, kind, (r, h) =>
            {
                var bytes = r.GetBlobBytes(h);
                return "'" + Encoding.UTF8.GetString(bytes, 0, bytes.Length) + "'";
            });
        }

        private string FieldSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.FieldSignature, (r, h) => Signature(r, h, BlobKind.FieldSignature));

        private string MethodSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.MethodSignature, (r, h) => Signature(r, h, BlobKind.MethodSignature));

        private string StandaloneSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.StandAloneSignature, (r, h) => Signature(r, h, BlobKind.StandAloneSignature));

        private string MemberReferenceSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.MemberRefSignature, (r, h) => Signature(r, h, BlobKind.MemberRefSignature));

        private string MethodSpecificationSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.MethodSpec, (r, h) => Signature(r, h, BlobKind.MethodSpec));

        private string TypeSpecificationSignature(Func<BlobHandle> getHandle) =>
            Literal(getHandle, BlobKind.TypeSpec, (r, h) => Signature(r, h, BlobKind.TypeSpec));

        public string MethodSignature(BlobHandle signatureHandle) =>
            Literal(signatureHandle, (r, h) => Signature(r, (BlobHandle)h, BlobKind.MethodSignature));

        public string StandaloneSignature(BlobHandle signatureHandle) =>
            Literal(signatureHandle, (r, h) => Signature(r, (BlobHandle)h, BlobKind.StandAloneSignature));

        public string MemberReferenceSignature(BlobHandle signatureHandle) =>
            Literal(signatureHandle, (r, h) => Signature(r, (BlobHandle)h, BlobKind.MemberRefSignature));

        public string MethodSpecificationSignature(BlobHandle signatureHandle) =>
            Literal(signatureHandle, (r, h) => Signature(r, (BlobHandle)h, BlobKind.MethodSpec));

        public string TypeSpecificationSignature(BlobHandle signatureHandle) =>
            Literal(signatureHandle, (r, h) => Signature(r, (BlobHandle)h, BlobKind.TypeSpec));

        private string Signature(MetadataReader reader, BlobHandle signatureHandle, BlobKind kind)
        {
            try
            {
                var sigReader = reader.GetBlobReader(signatureHandle);
                var decoder = new SignatureDecoder<string, object>(_signatureVisualizer, reader, genericContext: null);
                switch (kind)
                {
                    case BlobKind.FieldSignature:
                        return decoder.DecodeFieldSignature(ref sigReader);

                    case BlobKind.MethodSignature:
                        return MethodSignature(decoder.DecodeMethodSignature(ref sigReader));

                    case BlobKind.StandAloneSignature:
                        return string.Join(", ", decoder.DecodeLocalSignature(ref sigReader));

                    case BlobKind.MemberRefSignature:
                        var header = sigReader.ReadSignatureHeader();
                        sigReader.Offset = 0;
                        return header.Kind switch
                        {
                            SignatureKind.Field => decoder.DecodeFieldSignature(ref sigReader),
                            SignatureKind.Method => MethodSignature(decoder.DecodeMethodSignature(ref sigReader)),
                            _ => throw new BadImageFormatException(),
                        };

                    case BlobKind.MethodSpec:
                        return string.Join(", ", decoder.DecodeMethodSpecificationSignature(ref sigReader));

                    case BlobKind.TypeSpec:
                        return decoder.DecodeType(ref sigReader, allowTypeSpecifications: false);

                    default:
                        throw ExceptionUtilities.UnexpectedValue(kind);
                }
            }
            catch (BadImageFormatException)
            {
                try
                {
                    return $"<bad signature: {BitConverter.ToString(reader.GetBlobBytes(signatureHandle))}>";
                }
                catch (BadImageFormatException)
                {
                    return BadMetadataStr;
                }
            }
        }

        private static string MethodSignature(MethodSignature<string> signature)
        {
            var builder = new StringBuilder();
            builder.Append(signature.ReturnType);
            builder.Append(' ');
            builder.Append('(');

            for (int i = 0; i < signature.ParameterTypes.Length; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");

                    if (i == signature.RequiredParameterCount)
                    {
                        builder.Append("... ");
                    }
                }

                builder.Append(signature.ParameterTypes[i]);
            }

            builder.Append(')');
            return builder.ToString();
        }

        private string QualifiedTypeDefinitionName(TypeDefinitionHandle handle)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(handle, displayTable: true);
            }

            var builder = new StringBuilder();
            Recurse(handle, isLastPart: true);
            return builder.ToString();

            void Recurse(TypeDefinitionHandle typeDefinitionHandle, bool isLastPart)
            {
                try
                {
                    var generationDeclaringTypeDef = GetGenerationTypeDefinition(typeDefinitionHandle);
                    var declaringTypeDefHandle = _reader.GetTypeDefinition(typeDefinitionHandle).GetDeclaringType();

                    if (declaringTypeDefHandle.IsNil)
                    {
                        if (!generationDeclaringTypeDef.Namespace.IsNil)
                        {
                            builder.Append(GetString(generationDeclaringTypeDef.Namespace)).Append('.');
                        }
                    }
                    else
                    {
                        Recurse(declaringTypeDefHandle, isLastPart: false);
                    }

                    var name = GetString(generationDeclaringTypeDef.Name);
                    builder.Append(name);
                }
                catch (BadImageFormatException)
                {
                    builder.Append(BadMetadataStr);
                }

                if (!isLastPart)
                {
                    builder.Append('.');
                }
            }
        }

        private string QualifiedMethodName(MethodDefinitionHandle handle, TypeDefinitionHandle scope = default)
            => QualifiedMemberName(
                handle,
                scope,
                entity => entity.Name,
                entity => entity.GetDeclaringType(),
                (reader, handle) => reader.GetMethodDefinition((MethodDefinitionHandle)handle));

        private string QualifiedFieldName(FieldDefinitionHandle handle, TypeDefinitionHandle scope = default)
            => QualifiedMemberName(
                handle,
                scope,
                entity => entity.Name,
                entity => entity.GetDeclaringType(),
                (reader, handle) => reader.GetFieldDefinition((FieldDefinitionHandle)handle));

        private string QualifiedMemberName<TMemberEntity>(
            EntityHandle memberHandle,
            TypeDefinitionHandle scope,
            Func<TMemberEntity, StringHandle> nameGetter,
            Func<TMemberEntity, TypeDefinitionHandle> declaringTypeGetter,
            Func<MetadataReader, Handle, TMemberEntity> entityGetter)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(memberHandle, displayTable: true);
            }

            try
            {
                var member = GetGenerationEntity(memberHandle, entityGetter);
                var declaringTypeDefHandle = _encAddedMemberToParentMap.TryGetValue(memberHandle, out var parentHandle) ? (TypeDefinitionHandle)parentHandle : declaringTypeGetter(member);

                var typeQualification = declaringTypeDefHandle != scope && !declaringTypeDefHandle.IsNil ? QualifiedTypeDefinitionName(declaringTypeDefHandle) + "." : "";
                var memberName = GetString(nameGetter(member));

                return typeQualification + memberName;
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private string QualifiedMemberReferenceName(MemberReferenceHandle handle)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(handle, displayTable: true);
            }

            try
            {
                var memberReference = GetGenerationMemberReference(handle);
                return QualifiedName(memberReference.Parent) + "." + GetString(memberReference.Name);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private string QualifiedTypeReferenceName(TypeReferenceHandle handle)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(handle, displayTable: true);
            }

            try
            {
                var typeReference = GetGenerationTypeDefinition(handle);
                return GetString(typeReference.Namespace) + "." + GetString(typeReference.Name);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private string QualifiedMethodSpecificationName(MethodSpecificationHandle handle)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(handle, displayTable: true);
            }

            MethodSpecification methodSpecification;
            try
            {
                methodSpecification = GetGenerationMethodSpecification(handle);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            string qualifiedName;
            try
            {
                qualifiedName = QualifiedName(methodSpecification.Method);
            }
            catch (BadImageFormatException)
            {
                qualifiedName = BadMetadataStr;
            }

            string typeArguments;
            try
            {
                typeArguments = GetGenerationEntity(methodSpecification.Signature, (reader, handle) => Signature(reader, (BlobHandle)handle, BlobKind.MethodSpec));
            }
            catch (BadImageFormatException)
            {
                typeArguments = BadMetadataStr;
            }

            return qualifiedName + "<" + typeArguments + ">";
        }

        private string QualifiedTypeSpecificationName(TypeSpecificationHandle handle)
        {
            if (IsDelta && _aggregator == null)
            {
                return Token(handle, displayTable: true);
            }

            TypeSpecification typeSpecification;
            try
            {
                typeSpecification = GetGenerationTypeSpecification(handle);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            BlobHandle signature;
            try
            {
                signature = typeSpecification.Signature;
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            return GetGenerationEntity(signature, (reader, handle) => Signature(reader, (BlobHandle)handle, BlobKind.TypeSpec));
        }

        private string QualifiedName(EntityHandle handle, TypeDefinitionHandle scope = default)
            => handle.Kind switch
            {
                HandleKind.TypeDefinition => QualifiedTypeDefinitionName((TypeDefinitionHandle)handle),
                HandleKind.MethodDefinition => QualifiedMethodName((MethodDefinitionHandle)handle, scope),
                HandleKind.FieldDefinition => QualifiedFieldName((FieldDefinitionHandle)handle, scope),
                HandleKind.MemberReference => QualifiedMemberReferenceName((MemberReferenceHandle)handle),
                HandleKind.TypeReference => QualifiedTypeReferenceName((TypeReferenceHandle)handle),
                HandleKind.MethodSpecification => QualifiedMethodSpecificationName((MethodSpecificationHandle)handle),
                HandleKind.TypeSpecification => QualifiedTypeSpecificationName((TypeSpecificationHandle)handle),
                _ => null
            };

        private StringHandle MarkKind(StringHandle handle, StringKind kind)
        {
            if (!handle.IsNil && kind != StringKind.None)
            {
                _stringKinds[handle] = _stringKinds.TryGetValue(handle, out var existing) ? existing | kind : kind;
            }

            return handle;
        }

        private BlobHandle MarkKind(BlobHandle handle, BlobKind kind)
        {
            if (!handle.IsNil && kind != BlobKind.None)
            {
                _blobKinds[handle] = _blobKinds.TryGetValue(handle, out var existing) ? existing | kind : kind;
            }

            return handle;
        }

        private string Literal(Func<BlobHandle> getHandle, BlobKind kind) =>
            Literal(getHandle, kind, (r, h) => BitConverter.ToString(r.GetBlobBytes(h)));

        private string Literal(Func<BlobHandle> getHandle, BlobKind kind, Func<MetadataReader, BlobHandle, string> getValue)
        {
            BlobHandle handle;
            try
            {
                handle = MarkKind(getHandle(), kind);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            return Literal(handle, (r, h) => getValue(r, (BlobHandle)h));
        }

        private string Literal(Func<StringHandle> getHandle, StringKind kind) =>
            Literal(() => MarkKind(getHandle(), kind), (r, h) => "'" + StringUtilities.EscapeNonPrintableCharacters(r.GetString((StringHandle)h)) + "'");

        private string GetString(StringHandle handle) =>
            Literal(handle, (r, h) => r.GetString((StringHandle)h), noHeapReferences: true);

        private string GetString(UserStringHandle handle) =>
            Literal(handle, (r, h) => r.GetUserString((UserStringHandle)h), noHeapReferences: true);

        private string Literal(Func<GuidHandle> getHandle) =>
            Literal(() => getHandle(), (r, h) => "{" + r.GetGuid((GuidHandle)h) + "}");

        private string Literal(Func<Handle> getHandle, Func<MetadataReader, Handle, string> getValue)
        {
            Handle handle;
            try
            {
                handle = getHandle();
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            return Literal(handle, getValue);
        }

        private string Literal(Handle handle, Func<MetadataReader, Handle, string> getValue)
            => Literal(handle, getValue, NoHeapReferences);
        
        private string Literal(Handle handle, Func<MetadataReader, Handle, string> getValue, bool noHeapReferences)
        {
            if (handle.IsNil)
            {
                return "nil";
            }

            if (_aggregator != null)
            {
                Handle generationHandle;
                int generation;

                // workaround for https://github.com/dotnet/runtime/issues/113910
                if (handle.Kind == HandleKind.Guid)
                {
                    generationHandle = handle;
                    generation = -1;
                    
                    var guidOffset = MetadataTokens.GetHeapOffset(handle);
                    
                    for (int i = 0; i < _readers.Count; i++)
                    {
                        if (guidOffset <= _readers[i].GetHeapSize(HeapIndex.Guid) / 16)
                        {
                            generation = i;
                            break;
                        }
                    }

                    if (generation < 0)
                    {
                        return BadMetadataStr;
                    }
                }
                else
                {
                    generationHandle = _aggregator.GetGenerationHandle(handle, out generation);
                }

                var generationReader = _readers[generation];
                var offset = generationReader.GetHeapOffset(handle);
                var generationOffset = generationReader.GetHeapOffset(generationHandle);

                string value = GetValueChecked(getValue, generationReader, generationHandle);

                if (noHeapReferences)
                {
                    return value;
                }
                else if (offset == generationOffset)
                {
                    return $"{value} (#{offset:x})";
                }
                else
                {
                    return $"{value} (#{offset:x}/{generation}:{generationOffset:x})";
                }
            }

            if (IsDelta)
            {
                int aggregateHeapOffset = _reader.GetHeapOffset(handle);

                return TryDisplay(HandleKind.Blob, _blobHeapBaseOffset, o => MetadataTokens.BlobHandle(o)) ??
                       TryDisplay(HandleKind.String, _stringHeapBaseOffset, o => MetadataTokens.StringHandle(o)) ??
                       $"#{aggregateHeapOffset:x}";

                string TryDisplay(HandleKind kind, int heapBaseOffset, Func<int, Handle> getHandle)
                {
                    if (handle.Kind == kind && heapBaseOffset > 0)
                    {
                        // When reading delta without aggregator we assume that the string is in the current generation
                        // (this should be true as long as the compiler doesn't reuse strings emitted to previous generations).

                        var generationOffset = aggregateHeapOffset - heapBaseOffset;
                        try
                        {
                            var value = getValue(_reader, getHandle(generationOffset));
                            return $"{value} (#{generationOffset:x})";
                        }
                        catch (BadImageFormatException)
                        {
                            // fall through
                        }
                    }

                    return null;
                }
            }

            int heapOffset = MetadataTokens.GetHeapOffset(handle);

            // virtual heap handles don't have offset:
            bool displayHeapOffset = !noHeapReferences && heapOffset >= 0;

            return GetValueChecked(getValue, _reader, handle) + (displayHeapOffset ? $" (#{heapOffset:x})" : "");
        }

        private static string GetValueChecked(Func<MetadataReader, Handle, string> getValue, MetadataReader reader, Handle handle)
        {
            try
            {
                return getValue(reader, handle);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private static bool TryGetValue<T>(Func<T> getValue, out T result)
        {
            try
            {
                result = getValue();
                return true;
            }
            catch (BadImageFormatException)
            {
                result = default;
                return false;
            }
        }

        private static string TryRead<T>(ref BlobReader reader, ref bool error, FuncRef<BlobReader, T> read, Func<T, string> toString = null)
        {
            if (error)
            {
                return BadMetadataStr;
            }

            T value;
            try
            {
                value = read(ref reader);
            }
            catch (BadImageFormatException)
            {
                error = true;
                return BadMetadataStr;
            }

            return (toString != null) ? toString(value) : value.ToString();
        }

        private string ToString<TValue>(Func<TValue> getValue)
        {
            try
            {
                return getValue().ToString();
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private static string ToString<TValue>(Func<TValue> getValue, Func<TValue, string> valueToString)
        {
            try
            {
                return valueToString(getValue());
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private static string ToString<TValue, TArg>(Func<TValue> getValue, TArg arg, Func<TValue, TArg, string> valueToString)
        {
            try
            {
                return valueToString(getValue(), arg);
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }
        }

        private string Int32(Func<int> getValue)
            => ToString(getValue, value => value.ToString());

        private string Int32Hex(Func<int> getValue, int digits = 8)
            => ToString(getValue, value => "0x" + value.ToString("X" + digits));

        public string Token(Func<Handle> getHandle, bool displayTable = true)
            => ToString(getHandle, displayTable, Token);

        private string Token(Handle handle, bool displayTable = true)
        {
            string tokenStr = handle.IsNil ? "nil" : $"0x{_reader.GetToken(handle):x8}";

            if (_aggregator != null)
            {
                var generationHandle = (EntityHandle)_aggregator.GetGenerationHandle(handle, out int generation);
                if (generationHandle != handle)
                {
                    tokenStr += $"/{generation}:{MetadataTokens.GetRowNumber(generationHandle):x}";
                }
            }

            if (displayTable && MetadataTokens.TryGetTableIndex(handle.Kind, out var table))
            {
                return $"{tokenStr} ({table})";
            }
            else
            {
                return tokenStr;
            }
        }

        private string RowId(Func<EntityHandle> getHandle)
            => ToString(getHandle, RowId);

        private string RowId(EntityHandle handle)
            => handle.IsNil ? "nil" : $"#{_reader.GetRowNumber(handle):x}";

        private string HeapOffset(Func<Handle> getHandle)
            => ToString(getHandle, HeapOffset);

        private string HeapOffset(Handle handle)
            => handle.IsNil ? "nil" : NoHeapReferences ? "" : $"#{_reader.GetHeapOffset(handle):x}";

        private static string EnumValue<TIntegral>(Func<object> getValue) where TIntegral : IEquatable<TIntegral>
        {
            object value;

            try
            {
                value = getValue();
            }
            catch (BadImageFormatException)
            {
                return BadMetadataStr;
            }

            TIntegral integralValue = (TIntegral)value;
            if (integralValue.Equals(default))
            {
                return "0";
            }

            return $"0x{integralValue:x8} ({value})";
        }

        // TODO (tomat): handle collections should implement IReadOnlyCollection<Handle>
        private string TokenRange<THandle>(IReadOnlyCollection<THandle> handles, Func<THandle, EntityHandle> conversion)
        {
            var genericHandles = handles.Select(conversion);

            if (handles.Count < 0)
            {
                return "<bad token range>";
            }

            return (handles.Count == 0) ? "nil" : Token(() => genericHandles.First(), displayTable: false) + "-" + Token(() => genericHandles.Last(), displayTable: false);
        }

        // Fields and methods, and parameters in EnC deltas are not listed in TypeDef and MethodDef table, respectively.
        // Instead the EncLog table associates them with their containing TypeDefs/MethodDefs.
        private string MemberTokenRange<THandle>(IReadOnlyCollection<THandle> handles, Func<THandle, EntityHandle> conversion, int generation)
        {
            if (generation == 0)
            {
                return TokenRange(handles, conversion);
            }

            if (handles.Count == 0)
            {
                return "n/a (EnC)";
            }

            return TokenRange(handles, conversion) + " " + BadMetadataStr;
        }

        public string TokenList(IReadOnlyCollection<EntityHandle> handles, bool displayTable = false)
        {
            if (handles.Count == 0)
            {
                return "nil";
            }

            return string.Join(", ", handles.Select(h => Token(() => h, displayTable)));
        }

        private string Version(Func<Version> getVersion)
            => ToString(getVersion, version => version.Major + "." + version.Minor + "." + version.Build + "." + version.Revision);

        private string FormatAwaits(BlobHandle handle)
        {
            var sb = new StringBuilder();
            var blobReader = _reader.GetBlobReader(handle);

            while (blobReader.RemainingBytes > 0)
            {
                if (blobReader.Offset > 0)
                {
                    sb.Append(", ");
                }

                int value;
                sb.Append("(");
                sb.Append(blobReader.TryReadCompressedInteger(out value) ? value.ToString() : "?");
                sb.Append(", ");
                sb.Append(blobReader.TryReadCompressedInteger(out value) ? value.ToString() : "?");
                sb.Append(", ");
                sb.Append(blobReader.TryReadCompressedInteger(out value) ? Token(() => MetadataTokens.MethodDefinitionHandle(value)) : "?");
                sb.Append(')');
            }

            return sb.ToString();
        }

        private string FormatImports(ImportScope scope)
        {
            if (scope.ImportsBlob.IsNil)
            {
                return "nil";
            }

            var sb = new StringBuilder();

            foreach (var import in scope.GetImports())
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }

                switch (import.Kind)
                {
                    case ImportDefinitionKind.ImportNamespace:
                        sb.AppendFormat("{0}", LiteralUtf8Blob(() => import.TargetNamespace, BlobKind.ImportNamespace));
                        break;

                    case ImportDefinitionKind.ImportAssemblyNamespace:
                        sb.AppendFormat("{0}::{1}",
                            Token(() => import.TargetAssembly),
                            LiteralUtf8Blob(() => import.TargetNamespace, BlobKind.ImportNamespace));
                        break;

                    case ImportDefinitionKind.ImportType:
                        sb.AppendFormat("{0}", Token(() => import.TargetType));
                        break;

                    case ImportDefinitionKind.ImportXmlNamespace:
                        sb.AppendFormat("<{0} = {1}>",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias),
                            LiteralUtf8Blob(() => import.TargetNamespace, BlobKind.ImportNamespace));
                        break;

                    case ImportDefinitionKind.ImportAssemblyReferenceAlias:
                        sb.AppendFormat("Extern Alias {0}",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias));
                        break;

                    case ImportDefinitionKind.AliasAssemblyReference:
                        sb.AppendFormat("{0} = {1}",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias),
                            Token(() => import.TargetAssembly));
                        break;

                    case ImportDefinitionKind.AliasNamespace:
                        sb.AppendFormat("{0} = {1}",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias),
                            LiteralUtf8Blob(() => import.TargetNamespace, BlobKind.ImportNamespace));
                        break;

                    case ImportDefinitionKind.AliasAssemblyNamespace:
                        sb.AppendFormat("{0} = {1}::{2}",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias),
                            Token(() => import.TargetAssembly),
                            LiteralUtf8Blob(() => import.TargetNamespace, BlobKind.ImportNamespace));
                        break;

                    case ImportDefinitionKind.AliasType:
                        sb.AppendFormat("{0} = {1}",
                            LiteralUtf8Blob(() => import.Alias, BlobKind.ImportAlias),
                            Token(() => import.TargetType));
                        break;
                }
            }

            return sb.ToString();
        }

        private string SequencePoint(SequencePoint sequencePoint, bool includeDocument = true)
        {
            string range = sequencePoint.IsHidden ?
                "<hidden>" :
                $"({sequencePoint.StartLine}, {sequencePoint.StartColumn}) - ({sequencePoint.EndLine}, {sequencePoint.EndColumn})" +
                    (includeDocument ? $" [{RowId(() => sequencePoint.Document)}]" : "");

            return $"IL_{sequencePoint.Offset:X4}: " + range;
        }

        public void VisualizeHeaders()
        {
            _reader = _readers[0];

            _writer.WriteLine($"MetadataVersion: {_reader.MetadataVersion}");

            if (_reader.DebugMetadataHeader != null)
            {
                _writer.WriteLine("Id: " + BitConverter.ToString(_reader.DebugMetadataHeader.Id.ToArray()));

                if (!_reader.DebugMetadataHeader.EntryPoint.IsNil)
                {
                    _writer.WriteLine($"EntryPoint: {Token(() => _reader.DebugMetadataHeader.EntryPoint)}");
                }
            }

            _writer.WriteLine();
        }

        private TableBuilder ReadModuleTable()
        {
            var table = new TableBuilder(
                "Module (0x00):",
                "Gen",
                "Name",
                "Mvid",
                "EncId",
                "EncBaseId"
            );

            if (_reader.DebugMetadataHeader != null)
            {
                return table;
            }

            var def = _reader.GetModuleDefinition();

            table.AddRow(
                ToString(() => def.Generation),
                Literal(() => def.Name, StringKind.ModuleName),
                Literal(() => def.Mvid),
                Literal(() => def.GenerationId),
                Literal(() => def.BaseGenerationId));

            return table;
        }

        private TableBuilder ReadTypeRefTable()
        {
            var table = new TableBuilder(
                "TypeRef (0x01):",
                "Scope",
                "Name",
                "Namespace"
            );

            foreach (var handle in _reader.TypeReferences)
            {
                var entry = _reader.GetTypeReference(handle);

                table.AddRow(
                    Token(() => entry.ResolutionScope),
                    Literal(() => entry.Name, StringKind.TypeName),
                    Literal(() => entry.Namespace, StringKind.NamespaceName)
                );
            }

            return table;
        }

        private TableBuilder ReadTypeDefTable(int generation)
        {
            var table = new TableBuilder(
                "TypeDef (0x02):",
                "Name",
                "Namespace",
                "EnclosingType",
                "BaseType",
                "Interfaces",
                "Fields",
                "Methods",
                "Attributes",
                "ClassSize",
                "PackingSize"
            );

            foreach (var handle in _reader.TypeDefinitions)
            {
                var entry = _reader.GetTypeDefinition(handle);

                // EnclosingType and implemented Interfaces are stored in separate tables that reference this TypeDef via an aggregate handle.
                // Therefore we need to use aggregate TypeDefinition to look them up.
                var aggregateEntry = (generation > 0) ? _reader.GetTypeDefinition((TypeDefinitionHandle)GetAggregateHandle(handle, generation)) : entry;

                var layout = entry.GetLayout();

                // TODO: Visualize InterfaceImplementations
                var implementedInterfaces = aggregateEntry.GetInterfaceImplementations().Select(h => _reader.GetInterfaceImplementation(h).Interface).ToArray();

                table.AddRow(
                    Literal(() => entry.Name, StringKind.TypeName),
                    Literal(() => entry.Namespace, StringKind.NamespaceName),
                    Token(() => aggregateEntry.GetDeclaringType()),
                    Token(() => entry.BaseType),
                    TokenList(implementedInterfaces),
                    MemberTokenRange(entry.GetFields(), h => h, generation),
                    MemberTokenRange(entry.GetMethods(), h => h, generation),
                    EnumValue<int>(() => entry.Attributes),
                    !layout.IsDefault ? layout.Size.ToString() : "n/a",
                    !layout.IsDefault ? layout.PackingSize.ToString() : "n/a"
                );
            }

            return table;
        }

        private TableBuilder ReadFieldTable(int generation)
        {
            var table = new TableBuilder(
                "Field (0x04):",
                "Name",
                "Signature",
                "Attributes",
                "Marshalling",
                "Offset",
                "RVA"
            );

            foreach (var handle in _reader.FieldDefinitions)
            {
                var entry = _reader.GetFieldDefinition(handle);

                // Marshalling descriptor, field layout offset and field RVA are stored in separate tables that reference this FieldDef via an aggregate handle.
                // Therefore we need to use aggregate FieldDefinition to look them up.
                var aggregateEntry = (generation > 0) ? _reader.GetFieldDefinition((FieldDefinitionHandle)GetAggregateHandle(handle, generation)) : entry;

                table.AddRow(
                    Literal(() => entry.Name, StringKind.FieldName),
                    FieldSignature(() => entry.Signature),
                    EnumValue<int>(() => entry.Attributes),
                    Literal(() => aggregateEntry.GetMarshallingDescriptor(), BlobKind.Marshalling),
                    ToString(() =>
                    {
                        int offset = aggregateEntry.GetOffset();
                        return offset >= 0 ? offset.ToString() : "n/a";
                    }),
                    ToString(() => aggregateEntry.GetRelativeVirtualAddress())
                );
            }

            return table;
        }

        private TableBuilder ReadMethodTable(int generation)
        {
            var table = new TableBuilder(
                "Method (0x06, 0x1C):",
                "Name",
                "Signature",
                "RVA",
                "Parameters",
                "GenericParameters",
                "Attributes",
                "ImplAttributes",
                "ImportAttributes",
                "ImportName",
                "ImportModule"
            );

            foreach (var handle in _reader.MethodDefinitions)
            {
                var entry = _reader.GetMethodDefinition(handle);

                // GenericParameters and Import* are stored in separate tables that reference this MethodDef via an aggregate handle.
                // Therefore we need to use aggregate MethodDefinition to look them up.
                var aggregateEntry = (generation > 0) ? _reader.GetMethodDefinition((MethodDefinitionHandle)GetAggregateHandle(handle, generation)) : entry;

                var import = aggregateEntry.GetImport();

                table.AddRow(
                    Literal(() => entry.Name, StringKind.MethodName),
                    MethodSignature(() => entry.Signature),
                    Int32Hex(() => entry.RelativeVirtualAddress),
                    MemberTokenRange(entry.GetParameters(), h => h, generation),
                    TokenRange(aggregateEntry.GetGenericParameters(), h => h),
                    EnumValue<int>(() => entry.Attributes),    // TODO: we need better visualizer than the default enum
                    EnumValue<int>(() => entry.ImplAttributes),
                    EnumValue<short>(() => import.Attributes),
                    Literal(() => import.Name, StringKind.MethodImportName),
                    Token(() => import.Module)
                );
            }

            return table;
        }

        private TableBuilder ReadParamTable(int generation)
        {
            var table = new TableBuilder(
                "Param (0x08):",
                "Name",
                "Seq#",
                "Attributes",
                "Marshalling"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.Param); i <= count; i++)
            {
                var handle = MetadataTokens.ParameterHandle(i);
                var entry = _reader.GetParameter(handle);

                // Marshalling descriptor is stored in separate table that reference this Param via an aggregate handle.
                // Therefore we need to use aggregate Parameter to look them up.
                var aggregateEntry = (generation > 0) ? _reader.GetParameter((ParameterHandle)GetAggregateHandle(handle, generation)) : entry;

                table.AddRow(
                    Literal(() => entry.Name, StringKind.ParamName),
                    ToString(() => entry.SequenceNumber),
                    EnumValue<int>(() => entry.Attributes),
                    Literal(() => aggregateEntry.GetMarshallingDescriptor(), BlobKind.Marshalling)
                );
            }

            return table;
        }

        private TableBuilder ReadMemberRefTable()
        {
            var table = new TableBuilder(
                "MemberRef (0x0a):",
                "Parent",
                "Name",
                "Signature"
            );

            foreach (var handle in _reader.MemberReferences)
            {
                var entry = _reader.GetMemberReference(handle);

                table.AddRow(
                    Token(() => entry.Parent),
                    Literal(() => entry.Name, StringKind.MemberName),
                    MemberReferenceSignature(() => entry.Signature)
                );
            }

            return table;
        }

        private TableBuilder ReadConstantTable()
        {
            var table = new TableBuilder(
                "Constant (0x0b):",
                "Parent",
                "Type",
                "Value"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.Constant); i <= count; i++)
            {
                var entry = _reader.GetConstant(MetadataTokens.ConstantHandle(i));

                table.AddRow(
                    Token(() => entry.Parent),
                    EnumValue<byte>(() => entry.TypeCode),
                    Literal(() => entry.Value, BlobKind.ConstantValue)
                );
            }

            return table;
        }

        private TableBuilder ReadCustomAttributeTable()
        {
            var table = new TableBuilder(
                "CustomAttribute (0x0c):",
                "Parent",
                "Constructor",
                "Value"
            );

            foreach (var handle in _reader.CustomAttributes)
            {
                var entry = _reader.GetCustomAttribute(handle);

                table.AddRow(
                    Token(() => entry.Parent),
                    Token(() => entry.Constructor),
                    Literal(() => entry.Value, BlobKind.CustomAttribute)
                );
            }

            return table;
        }

        private TableBuilder ReadDeclSecurityTable()
        {
            var table = new TableBuilder(
                "DeclSecurity (0x0e):",
                "Parent",
                "PermissionSet",
                "Action"
            );

            foreach (var handle in _reader.DeclarativeSecurityAttributes)
            {
                var entry = _reader.GetDeclarativeSecurityAttribute(handle);

                table.AddRow(
                    Token(() => entry.Parent),
                    Literal(() => entry.PermissionSet, BlobKind.PermissionSet),
                    EnumValue<short>(() => entry.Action)
                );
            }

            return table;
        }

        private TableBuilder ReadStandAloneSigTable()
        {
            var table = new TableBuilder(
                "StandAloneSig (0x11):",
                "Signature"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.StandAloneSig); i <= count; i++)
            {
                var entry = _reader.GetStandaloneSignature(MetadataTokens.StandaloneSignatureHandle(i));
                table.AddRow(StandaloneSignature(() => entry.Signature));
            }

            return table;
        }

        private TableBuilder ReadEventTable()
        {
            var table = new TableBuilder(
                "Event (0x12, 0x14, 0x18):",
                "Name",
                "Add",
                "Remove",
                "Fire",
                "Attributes"
            );

            foreach (var handle in _reader.EventDefinitions)
            {
                var entry = _reader.GetEventDefinition(handle);
                var accessors = entry.GetAccessors();

                table.AddRow(
                    Literal(() => entry.Name, StringKind.EventName),
                    Token(() => accessors.Adder),
                    Token(() => accessors.Remover),
                    Token(() => accessors.Raiser),
                    EnumValue<int>(() => entry.Attributes)
                );
            }

            return table;
        }

        private TableBuilder ReadPropertyTable()
        {
            var table = new TableBuilder(
                "Property (0x15, 0x17, 0x18):",
                "Name",
                "Get",
                "Set",
                "Attributes"
            );

            foreach (var handle in _reader.PropertyDefinitions)
            {
                var entry = _reader.GetPropertyDefinition(handle);
                var accessors = entry.GetAccessors();

                table.AddRow(
                    Literal(() => entry.Name, StringKind.PropertyName),
                    Token(() => accessors.Getter),
                    Token(() => accessors.Setter),
                    EnumValue<int>(() => entry.Attributes)
                );
            }

            return table;
        }

        private TableBuilder ReadMethodImplTable()
        {
            var table = new TableBuilder(
                "MethodImpl (0x19):",
                "Type",
                "Body",
                "Declaration"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.MethodImpl); i <= count; i++)
            {
                var entry = _reader.GetMethodImplementation(MetadataTokens.MethodImplementationHandle(i));

                table.AddRow(
                    Token(() => entry.Type),
                    Token(() => entry.MethodBody),
                    Token(() => entry.MethodDeclaration)
                );
            }

            return table;
        }

        private TableBuilder ReadModuleRefTable()
        {
            var table = new TableBuilder(
                "ModuleRef (0x1a):",
                "Name"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.ModuleRef); i <= count; i++)
            {
                var entry = _reader.GetModuleReference(MetadataTokens.ModuleReferenceHandle(i));
                table.AddRow(Literal(() => entry.Name, StringKind.ModuleName));
            }

            return table;
        }

        private TableBuilder ReadTypeSpecTable()
        {
            var table = new TableBuilder(
                "TypeSpec (0x1b):",
                "Name");

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.TypeSpec); i <= count; i++)
            {
                var entry = _reader.GetTypeSpecification(MetadataTokens.TypeSpecificationHandle(i));
                table.AddRow(TypeSpecificationSignature(() => entry.Signature));
            }

            return table;
        }

        private TableBuilder ReadEnCLogTable()
        {
            var table = new TableBuilder(
                "EnC Log (0x1e):",
                "Entity",
                "Operation");

            foreach (var entry in _reader.GetEditAndContinueLogEntries())
            {
                table.AddRow(
                    Token(() => entry.Handle),
                    EnumValue<int>(() => entry.Operation));
            }

            return table;
        }

        private TableBuilder ReadEnCMapTable()
        {
            TableBuilder table;
            if (_aggregator != null)
            {
                table = new TableBuilder("EnC Map (0x1f):", "Entity", "Gen", "Row", "Edit");
            }
            else
            {
                table = new TableBuilder("EnC Map (0x1f):", "Entity");
            }

            foreach (var entry in _reader.GetEditAndContinueMapEntries())
            {
                if (_aggregator != null)
                {
                    int generation;
                    EntityHandle primary = (EntityHandle)_aggregator.GetGenerationHandle(entry, out generation);
                    bool isUpdate = _readers[generation] != _reader;

                    var primaryModule = _readers[generation].GetModuleDefinition();

                    table.AddRow(
                        Token(() => entry),
                        ToString(() => primaryModule.Generation),
                        "0x" + MetadataTokens.GetRowNumber(primary).ToString("x6"),
                        isUpdate ? "update" : "add");
                }
                else
                {
                    table.AddRow(Token(() => entry));
                }
            }

            return table;
        }

        private TableBuilder ReadAssemblyTable()
        {
            var table = new TableBuilder(
                "Assembly (0x20):",
                "Name",
                "Version",
                "Culture",
                "PublicKey",
                "Flags",
                "HashAlgorithm"
            );

            if (!_reader.IsAssembly)
            {
                return table;
            }

            var entry = _reader.GetAssemblyDefinition();

            table.AddRow(
                Literal(() => entry.Name, StringKind.AssemblyName),
                Version(() => entry.Version),
                Literal(() => entry.Culture, StringKind.CultureName),
                Literal(() => entry.PublicKey, BlobKind.Key),
                EnumValue<int>(() => entry.Flags),
                EnumValue<int>(() => entry.HashAlgorithm)
            );

            return table;
        }

        private TableBuilder ReadAssemblyRefTable()
        {
            var table = new TableBuilder(
                "AssemblyRef (0x23):",
                "Name",
                "Version",
                "Culture",
                "PublicKeyOrToken",
                "Flags"
            );

            foreach (var handle in _reader.AssemblyReferences)
            {
                var entry = _reader.GetAssemblyReference(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.AssemblyName),
                    Version(() => entry.Version),
                    Literal(() => entry.Culture, StringKind.CultureName),
                    Literal(() => entry.PublicKeyOrToken, BlobKind.Key),
                    EnumValue<int>(() => entry.Flags)
                );
            }

            return table;
        }

        private TableBuilder ReadFileTable()
        {
            var table = new TableBuilder(
                "File (0x26):",
                "Name",
                "Metadata",
                "HashValue"
            );

            foreach (var handle in _reader.AssemblyFiles)
            {
                var entry = _reader.GetAssemblyFile(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.FileName),
                    entry.ContainsMetadata ? "Yes" : "No",
                    Literal(() => entry.HashValue, BlobKind.FileHash)
                );
            }

            return table;
        }

        private TableBuilder ReadExportedTypeTable()
        {
            var table = new TableBuilder(
                "ExportedType (0x27):",
                "Name",
                "Namespace",
                "Attributes",
                "Implementation",
                "TypeDefinitionId"
            );

            const TypeAttributes TypeForwarder = (TypeAttributes)0x00200000;

            foreach (var handle in _reader.ExportedTypes)
            {
                var entry = _reader.GetExportedType(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.TypeName),
                    Literal(() => entry.Namespace, StringKind.NamespaceName),
                    ToString(() => ((entry.Attributes & TypeForwarder) == TypeForwarder ? "TypeForwarder, " : "") + (entry.Attributes & ~TypeForwarder).ToString()),
                    Token(() => entry.Implementation),
                    Int32Hex(() => entry.GetTypeDefinitionId())
                );
            }

            return table;
        }

        private TableBuilder ReadManifestResourceTable()
        {
            var table = new TableBuilder(
                "ManifestResource (0x28):",
                "Name",
                "Attributes",
                "Offset",
                "Implementation"
            );

            foreach (var handle in _reader.ManifestResources)
            {
                var entry = _reader.GetManifestResource(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.ResourceName),
                    ToString(() => entry.Attributes),
                    ToString(() => entry.Offset),
                    Token(() => entry.Implementation)
                );
            }

            return table;
        }

        private TableBuilder ReadGenericParamTable(int generation)
        {
            var table = new TableBuilder(
                "GenericParam (0x2a):",
                "Name",
                "Seq#",
                "Attributes",
                "Parent",
                "TypeConstraints"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.GenericParam); i <= count; i++)
            {
                var handle = MetadataTokens.GenericParameterHandle(i);
                var entry = _reader.GetGenericParameter(handle);

                // Type constraints are stored in separate table that reference this GenericParam via an aggregate handle.
                // Therefore we need to use aggregate GenericParameter to look them up.
                var aggregateEntry = (generation > 0) ? _reader.GetGenericParameter((GenericParameterHandle)GetAggregateHandle(handle, generation)) : entry;

                table.AddRow(
                    Literal(() => entry.Name, StringKind.GenericParamName),
                    ToString(() => entry.Index),
                    EnumValue<int>(() => entry.Attributes),
                    Token(() => entry.Parent),
                    TokenRange(aggregateEntry.GetConstraints(), h => h)
                );
            }

            return table;
        }

        private TableBuilder ReadMethodSpecTable()
        {
            var table = new TableBuilder(
                "MethodSpec (0x2b):",
                "Method",
                "Signature"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.MethodSpec); i <= count; i++)
            {
                var entry = _reader.GetMethodSpecification(MetadataTokens.MethodSpecificationHandle(i));

                table.AddRow(
                    Token(() => entry.Method),
                    MethodSpecificationSignature(() => entry.Signature)
                );
            }

            return table;
        }

        private TableBuilder ReadGenericParamConstraintTable()
        {
            var table = new TableBuilder(
                "GenericParamConstraint (0x2c):",
                "Parent",
                "Type"
            );

            for (int i = 1, count = _reader.GetTableRowCount(TableIndex.GenericParamConstraint); i <= count; i++)
            {
                var entry = _reader.GetGenericParameterConstraint(MetadataTokens.GenericParameterConstraintHandle(i));

                table.AddRow(
                    Token(() => entry.Parameter),
                    Token(() => entry.Type)
                );
            }

            return table;
        }

        private void WriteUserStrings()
        {
            int size = _reader.GetHeapSize(HeapIndex.UserString);
            if (size == 0)
            {
                return;
            }

            // TODO: the heap is aligned, don't display the trailing empty strings
            _writer.WriteLine($"#US (size = {size}):");
            var handle = MetadataTokens.UserStringHandle(0);
            do
            {
                string value = StringUtilities.EscapeNonPrintableCharacters(_reader.GetUserString(handle));
                _writer.WriteLine($"  {_reader.GetHeapOffset(handle):x}: '{value}'");
                handle = _reader.GetNextHandle(handle);
            }
            while (!handle.IsNil);

            _writer.WriteLine();
        }

        private int GetHeapBaseOffset<THandle>(int generation, HeapIndex heapIndex, IReadOnlyCollection<THandle> handles, Func<THandle, int> getHeapOffset)
        {
            if (!IsDelta)
            {
                return 0;
            }

            if (_aggregator != null)
            {
                return _readers[generation].GetHeapSize(heapIndex);
            }

            if (handles.Count == 0)
            {
                return 0;
            }

            // Heuristics for the scenario when we do not have all previous generation readers.
            // Assuming the compiler does not reuse any strings/blobs from the previous generation,
            // the minimal heap offset used the metadata tables of this generation will be the 
            // base offset of the heap.
            //
            // -1 accounts for the empty string/blob that's always emitted at the start of the #String/#Blob heap.
            return handles.Min(handle => getHeapOffset(handle)) - 1;
        }

        private void WriteStrings(int generation)
        {
            int size = _reader.GetHeapSize(HeapIndex.String);
            if (size == 0)
            {
                return;
            }

            int baseOffset = GetHeapBaseOffset(generation, HeapIndex.String, _stringKinds.Keys, MetadataTokens.GetHeapOffset);

            _writer.WriteLine($"#String (size = {size}):");
            var handle = MetadataTokens.StringHandle(0);
            do
            {
                var value = StringUtilities.EscapeNonPrintableCharacters(_reader.GetString(handle));

                var aggregateOffset = baseOffset + MetadataTokens.GetHeapOffset(handle);
                var kindDisplay = _stringKinds.TryGetValue(MetadataTokens.StringHandle(aggregateOffset), out var kind) ? $" ({kind})" : "";

                _writer.WriteLine($"  {aggregateOffset:x}: '{value}'{kindDisplay}");
                handle = _reader.GetNextHandle(handle);
            }
            while (!handle.IsNil);

            _writer.WriteLine();
        }

        private void WriteBlobs(int generation)
        {
            int heapSize = _reader.GetHeapSize(HeapIndex.Blob);
            if (heapSize == 0)
            {
                return;
            }

            int baseOffset = GetHeapBaseOffset(generation, HeapIndex.Blob, _blobKinds.Keys, MetadataTokens.GetHeapOffset);

            var heapOffset = _reader.GetHeapMetadataOffset(HeapIndex.Blob);
            bool hasBadMetadata = false;

            _writer.WriteLine($"#Blob (size = {heapSize}):");
            var handle = MetadataTokens.BlobHandle(0);
            do
            {
                byte[] value;

                int offset = _reader.GetHeapOffset(handle);
                int aggregateOffset = baseOffset + offset;

                string valueDisplay = "";

                try
                {
                    value = _reader.GetBlobBytes(handle);
                }
                catch
                {
                    hasBadMetadata = true;

                    unsafe
                    {
                        var blobHeapReader = new BlobReader(_reader.MetadataPointer + heapOffset + offset, heapSize - offset);

                        int blobSize;
                        try
                        {
                            blobSize = blobHeapReader.ReadCompressedInteger();
                        }
                        catch
                        {
                            blobSize = -1;
                        }

                        value = blobHeapReader.ReadBytes(blobHeapReader.RemainingBytes);
                        valueDisplay = $"{BadMetadataStr} size: {((blobSize == -1) ? "?" : blobSize.ToString())}, remaining bytes: ";
                    }
                }

                var kindDisplay = _blobKinds.TryGetValue(MetadataTokens.BlobHandle(aggregateOffset), out var kind) ? $" ({kind})" : "";

                if (value.Length > 0)
                {
                    int displayLength = (_options & MetadataVisualizerOptions.ShortenBlobs) != 0 ? Math.Min(4, value.Length) : value.Length;
                    valueDisplay += BitConverter.ToString(value, 0, displayLength) + (displayLength < value.Length ? "-..." : null);
                }
                else
                {
                    valueDisplay += "<empty>";
                }

                _writer.WriteLine($"  {aggregateOffset:x}: {valueDisplay}{kindDisplay}");

                if (hasBadMetadata)
                {
                    break;
                }

                handle = _reader.GetNextHandle(handle);
            }
            while (!handle.IsNil);
        }

        private void WriteCustomAttributeSizes()
        {
            // don't calculate statistics for EnC delta, it's not interesting
            if (!IsDelta)
            {
                _writer.WriteLine();
                _writer.WriteLine("CustomAttribute sizes by constructor:");
                try
                {
                    foreach (var grouping in from caHandle in _reader.CustomAttributes
                                             let ca = _reader.GetCustomAttribute(caHandle)
                                             group ca.Constructor by ca.Value into values   // blob -> { ctor1, ctor2, ... }
                                             group values.Key by values.First() into g      // ctor1 -> { blob1, ... }
                                             select new { Ctor = g.Key, Size = g.Sum(ca => _reader.GetBlobReader(ca).Length) } into ctorAndSize
                                             orderby ctorAndSize.Size descending
                                             select ctorAndSize)
                    {
                        string typeStr = null;
                        switch (grouping.Ctor.Kind)
                        {
                            case HandleKind.MemberReference:
                                var memberRef = _reader.GetMemberReference((MemberReferenceHandle)grouping.Ctor);

                                switch (memberRef.Parent.Kind)
                                {
                                    case HandleKind.TypeReference:
                                        var typeRef = _reader.GetTypeReference((TypeReferenceHandle)memberRef.Parent);
                                        typeStr = typeRef.Namespace.IsNil ? _reader.GetString(typeRef.Name) : _reader.GetString(typeRef.Namespace) + "." + _reader.GetString(typeRef.Name);
                                        break;

                                    case HandleKind.TypeDefinition:
                                        var typeDef = _reader.GetTypeDefinition((TypeDefinitionHandle)memberRef.Parent);
                                        typeStr = typeDef.Namespace.IsNil ? _reader.GetString(typeDef.Name) : _reader.GetString(typeDef.Namespace) + "." + _reader.GetString(typeDef.Name);
                                        break;

                                    case HandleKind.MethodDefinition:
                                    case HandleKind.ModuleReference:
                                    case HandleKind.TypeSpecification:
                                        break;
                                }

                                break;

                            case HandleKind.MethodDefinition:
                                // TODO
                                break;
                        }


                        // grouping.Key
                        _writer.WriteLine($"  {typeStr ?? Token(() => grouping.Ctor)}: {grouping.Size} bytes");
                    }
                }
                catch (BadImageFormatException)
                {
                    _writer.WriteLine(BadMetadataStr);
                }

                _writer.WriteLine();
            }
        }

        private void WriteGuids()
        {
            int size = _reader.GetHeapSize(HeapIndex.Guid);
            if (size == 0)
            {
                return;
            }

            _writer.WriteLine(string.Format("#Guid (size = {0}):", size));
            int i = 1;
            while (i <= size / 16)
            {
                string value = _reader.GetGuid(MetadataTokens.GuidHandle(i)).ToString();
                _writer.WriteLine("  {0:x}: {{{1}}}", i, value);
                i++;
            }

            _writer.WriteLine();
        }

        public void WriteDocument()
          => WriteTable(ReadDocumentTable());

        private TableBuilder ReadDocumentTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.Document),
                "Name",
                "Language",
                "HashAlgorithm",
                "Hash"
            );

            foreach (var handle in _reader.Documents)
            {
                var entry = _reader.GetDocument(handle);

                table.AddRow(
                    DocumentName(() => entry.Name),
                    Language(() => entry.Language),
                    HashAlgorithm(() => entry.HashAlgorithm),
                    Literal(() => entry.Hash, BlobKind.DocumentHash)
               );
            }

            return table;
        }

        public void WriteMethodDebugInformation()
          => WriteTable(ReadMethodDebugInformationTable());

        private TableBuilder ReadMethodDebugInformationTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.MethodDebugInformation),
                "IL"
            );

            if (_reader.MethodDebugInformation.Count == 0)
            {
                return table;
            }

            var detailsBuilder = new StringBuilder();

            foreach (var handle in _reader.MethodDebugInformation)
            {
                if (handle.IsNil)
                {
                    continue;
                }

                var entry = _reader.GetMethodDebugInformation(handle);

                bool hasSingleDocument = false;
                bool hasSequencePoints = false;
                try
                {
                    hasSingleDocument = !entry.Document.IsNil;
                    hasSequencePoints = !entry.SequencePointsBlob.IsNil;
                }
                catch (BadImageFormatException)
                {
                    hasSingleDocument = hasSequencePoints = false;
                }

                string details;

                if (hasSequencePoints)
                {
                    _blobKinds[entry.SequencePointsBlob] = BlobKind.SequencePoints;

                    detailsBuilder.Clear();
                    detailsBuilder.AppendLine("{");

                    bool addLineBreak = false;

                    if (!TryGetValue(() => entry.GetStateMachineKickoffMethod(), out var kickoffMethod) || !kickoffMethod.IsNil)
                    {
                        detailsBuilder.AppendLine($"  Kickoff Method: {(kickoffMethod.IsNil ? BadMetadataStr : Token(kickoffMethod))}");
                        addLineBreak = true;
                    }

                    if (!TryGetValue(() => entry.LocalSignature, out var localSignature) || !localSignature.IsNil)
                    {
                        detailsBuilder.AppendLine($"  Locals: {(localSignature.IsNil ? BadMetadataStr : Token(localSignature))}");
                        addLineBreak = true;
                    }

                    if (hasSingleDocument)
                    {
                        detailsBuilder.AppendLine($"  Document: {RowId(() => entry.Document)}");
                        addLineBreak = true;
                    }

                    if (addLineBreak)
                    {
                        detailsBuilder.AppendLine();
                    }

                    try
                    {
                        foreach (var sequencePoint in entry.GetSequencePoints())
                        {
                            detailsBuilder.Append("  ");
                            detailsBuilder.AppendLine(SequencePoint(sequencePoint, includeDocument: !hasSingleDocument));
                        }
                    }
                    catch (BadImageFormatException)
                    {
                        detailsBuilder.AppendLine("  " + BadMetadataStr);
                    }

                    detailsBuilder.AppendLine("}");
                    details = detailsBuilder.ToString();
                }
                else
                {
                    details = null;
                }

                table.AddRowWithDetails(new[] { HeapOffset(() => entry.SequencePointsBlob) }, details);
            }

            return table;
        }

        public void WriteLocalScope()
          => WriteTable(ReadLocalScopeTable());

        private TableBuilder ReadLocalScopeTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.LocalScope),
                "Method",
                "ImportScope",
                "Variables",
                "Constants",
                "StartOffset",
                "Length"
            );

            foreach (var handle in _reader.LocalScopes)
            {
                var entry = _reader.GetLocalScope(handle);

                table.AddRow(
                    Token(() => entry.Method),
                    Token(() => entry.ImportScope),
                    TokenRange(entry.GetLocalVariables(), h => h),
                    TokenRange(entry.GetLocalConstants(), h => h),
                    Int32Hex(() => entry.StartOffset, digits: 4),
                    Int32(() => entry.Length)
               );
            }

            return table;
        }

        public void WriteLocalVariable()
          => WriteTable(ReadLocalVariableTable());

        private TableBuilder ReadLocalVariableTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.LocalVariable),
                "Name",
                "Index",
                "Attributes"
            );

            foreach (var handle in _reader.LocalVariables)
            {
                var entry = _reader.GetLocalVariable(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.LocalVariableName),
                    Int32(() => entry.Index),
                    EnumValue<int>(() => entry.Attributes)
               );
            }

            return table;
        }

        public void WriteLocalConstant()
          => WriteTable(ReadLocalConstantTable());

        private TableBuilder ReadLocalConstantTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.LocalConstant),
                "Name",
                "Signature"
            );

            foreach (var handle in _reader.LocalConstants)
            {
                var entry = _reader.GetLocalConstant(handle);

                table.AddRow(
                    Literal(() => entry.Name, StringKind.ConstantName),
                    Literal(() => entry.Signature, BlobKind.LocalConstantSignature, (r, h) => FormatLocalConstant(r, (BlobHandle)h))
               );
            }

            return table;
        }

        private SignatureTypeCode ReadConstantTypeCode(ref BlobReader sigReader, List<string> modifiers)
        {
            while (true)
            {
                var s = sigReader.ReadSignatureTypeCode();
                if (s == SignatureTypeCode.OptionalModifier || s == SignatureTypeCode.RequiredModifier)
                {
                    var type = sigReader.ReadTypeHandle();
                    modifiers.Add((s == SignatureTypeCode.RequiredModifier ? "modreq" : "modopt") + "(" + Token(() => type) + ")");
                }
                else
                {
                    return s;
                }
            }
        }

        private string FormatLocalConstant(MetadataReader reader, BlobHandle signature)
        {
            var sigReader = reader.GetBlobReader(signature);

            var modifiers = new List<string>();

            SignatureTypeCode typeCode = ReadConstantTypeCode(ref sigReader, modifiers);

            Handle typeHandle = default;
            object value;
            if (IsPrimitiveType(typeCode))
            {
                if (typeCode == SignatureTypeCode.String)
                {
                    if (sigReader.RemainingBytes == 1)
                    {
                        value = (sigReader.ReadByte() == 0xff) ? "null" : BadMetadataStr;
                    }
                    else if (sigReader.RemainingBytes % 2 != 0)
                    {
                        value = BadMetadataStr;
                    }
                    else
                    {
                        value = "'" + StringUtilities.EscapeNonPrintableCharacters(sigReader.ReadUTF16(sigReader.RemainingBytes)) + "'";
                    }
                }
                else
                {
                    object rawValue = sigReader.ReadConstant((ConstantTypeCode)typeCode);
                    if (rawValue is char c)
                    {
                        value = "'" + StringUtilities.EscapeNonPrintableCharacters(c.ToString()) + "'";
                    }
                    else
                    {
                        value = string.Format(CultureInfo.InvariantCulture, "{0}", rawValue);
                    }
                }

                if (sigReader.RemainingBytes > 0)
                {
                    typeHandle = sigReader.ReadTypeHandle();
                }
            }
            else if (typeCode == SignatureTypeCode.TypeHandle)
            {
                typeHandle = sigReader.ReadTypeHandle();
                value = (sigReader.RemainingBytes > 0) ? BitConverter.ToString(sigReader.ReadBytes(sigReader.RemainingBytes)) : "default";
            }
            else
            {
                value = (typeCode == SignatureTypeCode.Object) ? "null" : $"<bad type code: {typeCode}>";
            }

            return string.Format("{0} [{1}{2}]",
                value,
                 string.Join(" ", modifiers),
                typeHandle.IsNil ? typeCode.ToString() : Token(() => typeHandle));
        }

        public static string FormatBytes(byte[] blob, int sizeLimit = 32)
        {
            int length = blob.Length;
            string suffix = "";

            if (blob.Length > sizeLimit)
            {
                length = sizeLimit;
                suffix = "-...";
            }

            return BitConverter.ToString(blob, 0, length) + suffix;
        }

        private static bool IsPrimitiveType(SignatureTypeCode typeCode)
        {
            switch (typeCode)
            {
                case SignatureTypeCode.Boolean:
                case SignatureTypeCode.Char:
                case SignatureTypeCode.SByte:
                case SignatureTypeCode.Byte:
                case SignatureTypeCode.Int16:
                case SignatureTypeCode.UInt16:
                case SignatureTypeCode.Int32:
                case SignatureTypeCode.UInt32:
                case SignatureTypeCode.Int64:
                case SignatureTypeCode.UInt64:
                case SignatureTypeCode.Single:
                case SignatureTypeCode.Double:
                case SignatureTypeCode.String:
                    return true;

                default:
                    return false;
            }
        }

        private TableBuilder ReadImportScopeTable()
        {
            var table = new TableBuilder(
                MakeTableName(TableIndex.ImportScope),
                "Parent",
                "Imports"
            );

            foreach (var handle in _reader.ImportScopes)
            {
                var entry = _reader.GetImportScope(handle);

                _blobKinds[entry.ImportsBlob] = BlobKind.Imports;

                table.AddRow(
                    Token(() => entry.Parent),
                    FormatImports(entry)
               );
            }

            return table;
        }

        public void WriteCustomDebugInformation()
            => WriteTable(ReadCustomDebugInformationTable());

        private TableBuilder ReadCustomDebugInformationTable()
        {
            const int BlobSizeLimit = 32;

            var table = new TableBuilder(
                MakeTableName(TableIndex.CustomDebugInformation),
                "Parent",
                "Kind",
                "Value"
            );

            foreach (var handle in _reader.CustomDebugInformation)
            {
                var entry = _reader.GetCustomDebugInformation(handle);

                table.AddRowWithDetails(
                    fields: new[]
                    {
                        Token(() => entry.Parent),
                        CustomDebugInformationKind(() => entry.Kind),
                        Literal(() => entry.Value, BlobKind.CustomDebugInformation, (r, h) => FormatBytes(r.GetBlobBytes(h), BlobSizeLimit))
                    },
                    details: TryDecodeCustomDebugInformation(entry)
                );
            }

            return table;
        }

        public string TryDecodeCustomDebugInformation(CustomDebugInformation entry)
        {
            Guid kind;
            BlobReader blobReader;

            try
            {
                kind = _reader.GetGuid(entry.Kind);
                blobReader = _reader.GetBlobReader(entry.Value);
            }
            catch
            {
                // error is already reported
                return null;
            }

            if (kind == PortableCustomDebugInfoKinds.AsyncMethodSteppingInformationBlob)
            {
                return TryDecodeAsyncMethodSteppingInformation(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.SourceLink)
            {
                return VisualizeSourceLink(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.CompilationMetadataReferences)
            {
                return VisualizeCompilationMetadataReferences(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.CompilationOptions)
            {
                return VisualizeCompilationOptions(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.EmbeddedSource && _options.HasFlag(MetadataVisualizerOptions.EmbeddedSource))
            {
                return VisualizeEmbeddedSource(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.TypeDefinitionDocuments)
            {
                return VisualizeTypeDefinitionDocuments(blobReader);
            }

            if (kind == PortableCustomDebugInfoKinds.PrimaryConstructorInformationBlob)
            {
                // the blob is empty
                return null;
            }

            return null;
        }

        private static string TryDecodeAsyncMethodSteppingInformation(BlobReader reader)
        {
            var builder = new StringBuilder();
            builder.AppendLine("{");

            bool error = false;
            var catchHandlerOffsetStr = TryRead(ref reader, ref error, (ref BlobReader reader) => reader.ReadUInt32(), value => (value == 0) ? null : $"0x{value - 1:X4}");
                
            if (catchHandlerOffsetStr != null)
            {
                builder.AppendLine($"  CatchHandlerOffset: {catchHandlerOffsetStr}");
            }

            if (error) goto onError;

            while (reader.RemainingBytes > 0)
            {
                var yieldOffsetStr = TryRead(ref reader, ref error, (ref BlobReader reader) => reader.ReadUInt32(), value => $"0x{value:X4}");
                var resumeOffsetStr = TryRead(ref reader, ref error, (ref BlobReader reader) => reader.ReadUInt32(), value => $"0x{value:X4}");
                var moveNextMethodRowIdStr = TryRead(ref reader, ref error, (ref BlobReader reader) => reader.ReadCompressedInteger(), value => $"0x06{value:X6}");

                builder.AppendLine($"  Yield: {yieldOffsetStr}, Resume: {resumeOffsetStr}, MoveNext: {moveNextMethodRowIdStr}");
                if (error) goto onError;
            }

            onError:

            if (error)
            {
                builder.AppendLine("  Remaining bytes: " + FormatBytes(reader.ReadBytes(reader.RemainingBytes)));
            }

            builder.AppendLine("}");
            return builder.ToString();
        }

        private static string VisualizeSourceLink(BlobReader reader)
            => reader.ReadUTF8(reader.RemainingBytes) + Environment.NewLine;

        private static string TryReadUtf8NullTerminated(ref BlobReader reader)
        {
            var terminatorIndex = reader.IndexOf(0);
            if (terminatorIndex == -1)
            {
                return null;
            }

            var value = reader.ReadUTF8(terminatorIndex);
            _ = reader.ReadByte();
            return value;
        }

        [Flags]
        private enum MetadataReferenceFlags
        {
            Assembly = 1,
            EmbedInteropTypes = 1 << 1,
        }

        private static string VisualizeCompilationMetadataReferences(BlobReader reader)
        {
            var table = new TableBuilder(
                title: null,
                "FileName",
                "Aliases",
                "Flags",
                "TimeStamp",
                "FileSize",
                "MVID")
            {
                HorizontalSeparatorChar = '-',
                Indent = "  ",
                FirstRowNumber = 0,
            };

            while (reader.RemainingBytes > 0)
            {
                var fileName = TryReadUtf8NullTerminated(ref reader);
                var aliases = TryReadUtf8NullTerminated(ref reader);

                string flags = null;
                string timeStamp = null;
                string fileSize = null;
                string mvid = null;
                
                try { flags = ((MetadataReferenceFlags)reader.ReadByte()).ToString(); } catch { }
                try { timeStamp = $"0x{reader.ReadUInt32():X8}"; } catch { }
                try { fileSize = $"0x{reader.ReadUInt32():X8}"; } catch { }
                try { mvid = reader.ReadGuid().ToString(); } catch { }

                table.AddRow(
                    (fileName != null) ? $"'{fileName}'" : BadMetadataStr,
                    (aliases != null) ? $"'{string.Join("', '", aliases.Split(','))}'" : BadMetadataStr,
                    flags ?? BadMetadataStr,
                    timeStamp ?? BadMetadataStr,
                    fileSize ?? BadMetadataStr,
                    mvid ?? BadMetadataStr
                );
            }

            var builder = new StringBuilder();
            builder.AppendLine("{");
            table.WriteTo(new StringWriter(builder));
            builder.AppendLine("}");
            return builder.ToString();
        }

        private static string VisualizeCompilationOptions(BlobReader reader)
        {
            var builder = new StringBuilder();
            builder.AppendLine("{");

            while (reader.RemainingBytes > 0)
            {
                var key = TryReadUtf8NullTerminated(ref reader);
                if (key == null)
                {
                    builder.AppendLine(BadMetadataStr);
                    break;
                }

                builder.Append($"  {key}: ");

                var value = TryReadUtf8NullTerminated(ref reader);
                if (value == null)
                {
                    builder.AppendLine(BadMetadataStr);
                    break;
                }

                builder.AppendLine(value);
            }

            builder.AppendLine("}");
            return builder.ToString();
        }

        private static string VisualizeEmbeddedSource(BlobReader reader)
        {
            var builder = new StringBuilder();
            builder.AppendLine(">>>");
            int format = -1;
            try { format = reader.ReadInt32(); } catch { };
            byte[] bytes = null;
            try { bytes = reader.ReadBytes(reader.RemainingBytes); } catch { };

            if (format > 0 && bytes != null)
            {
                try
                {
                    using var compressedStream = new MemoryStream(bytes, writable: false);
                    using var uncompressedStream = new MemoryStream();
                    using var deflate = new DeflateStream(compressedStream, CompressionMode.Decompress);
                    deflate.CopyTo(uncompressedStream);
                    bytes = uncompressedStream.ToArray();
                }
                catch
                {
                    bytes = null;
                };
            }

            if (format < 0 || bytes == null)
            {
                builder.AppendLine(BadMetadataStr);
            }
            else
            {
                builder.AppendLine(Encoding.UTF8.GetString(bytes));
                builder.AppendLine("<<< End of Embedded Source");
            }
            return builder.ToString();
        }

        private string VisualizeTypeDefinitionDocuments(BlobReader reader)
        {
            var builder = new StringBuilder();
            builder.Append("Documents: {");

            var first = true;
            while (reader.RemainingBytes > 0)
            {
                if (!first)
                {
                    builder.Append(", ");
                }

                int? documentRowId = null;
                try { documentRowId = reader.ReadCompressedInteger(); } catch { }

                if (documentRowId == null)
                {
                    builder.Append(BadMetadataStr);
                    break;
                }

                builder.Append(Token(MetadataTokens.DocumentHandle(documentRowId.Value), displayTable: false));
                first = false;
            }

            builder.AppendLine("}");
            return builder.ToString();
        }

        public void VisualizeMethodBody(MethodBodyBlock body, MethodDefinitionHandle generationHandle, int generation)
        {
            var handle = (MethodDefinitionHandle)GetAggregateHandle(generationHandle, generation);
            VisualizeMethodBody(body, handle);
        }

        public void VisualizeMethodBody(MethodDefinitionHandle methodHandle, Func<int, MethodBodyBlock> bodyProvider)
        {
            var method = GetGenerationMethodDefinition(methodHandle);

            if ((method.ImplAttributes & MethodImplAttributes.CodeTypeMask) != MethodImplAttributes.Managed)
            {
                _writer.WriteLine("native code");
                return;
            }

            var rva = method.RelativeVirtualAddress;
            if (rva == 0)
            {
                return;
            }

            var body = bodyProvider(rva);
            VisualizeMethodBody(body, methodHandle);
        }

        private void VisualizeMethodBody(MethodBodyBlock body, MethodDefinitionHandle methodHandle)
        {
            var builder = new StringBuilder();

            var token = Token(methodHandle, displayTable: false);
            builder.AppendLine($"Method '{StringUtilities.EscapeNonPrintableCharacters(QualifiedMethodName(methodHandle))}' ({token})");

            if (!body.LocalSignature.IsNil)
            {
                builder.AppendLine($"  Locals: {StandaloneSignature(() => GetGenerationLocalSignature(body.LocalSignature).Signature)}");
            }

            var declaringTypeDefHandle = _encAddedMemberToParentMap.TryGetValue(methodHandle, out var parentHandle) ? 
                (TypeDefinitionHandle)parentHandle :
                GetGenerationMethodDefinition(methodHandle).GetDeclaringType();

            new ILVisualizer(this, scope: declaringTypeDefHandle).DumpMethod(
                builder,
                body.MaxStack,
                body.GetILContent(),
                ImmutableArray.Create<ILVisualizer.LocalInfo>(),     // TODO
                ImmutableArray.Create<ILVisualizer.HandlerSpan>());  // TODO: ILVisualizer.GetHandlerSpans(body.ExceptionRegions)

            builder.AppendLine();

            _writer.Write(builder.ToString());
        }

        public void WriteLine(string line)
        {
            _writer.WriteLine(line);
        }

        private sealed class TokenTypeComparer : IComparer<EntityHandle>
        {
            public static readonly TokenTypeComparer Instance = new TokenTypeComparer();

            public int Compare(EntityHandle x, EntityHandle y)
            {
                return x.Kind.CompareTo(y.Kind);
            }
        }
    }
}
