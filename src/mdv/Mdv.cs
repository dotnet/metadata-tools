// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using System.Text;
using Roslyn.Utilities;

namespace Microsoft.Metadata.Tools
{
    internal sealed class Mdv : IDisposable
    {
        private sealed class GenerationData
        {
            public readonly IDisposable MemoryOwner;
            public readonly MetadataReader MetadataReader;
            public readonly PEReader PEReaderOpt;
            public readonly byte[] ILDeltaOpt;

            public GenerationData(IDisposable memoryOwner, MetadataReader metadataReader, PEReader peReader = null, byte[] ilDelta = null)
            {
                MemoryOwner = memoryOwner;
                MetadataReader = metadataReader;
                PEReaderOpt = peReader;
                ILDeltaOpt = ilDelta;
            }
        }

        private readonly Arguments _arguments;
        private readonly TextWriter _writer;

        private static readonly (string Name, Func<PEHeader, DirectoryEntry> Accessor)[] s_peDataDirectories =
        {
            ("ExportTable", header => header.ExportTableDirectory),
            ("ImportTable", header => header.ImportTableDirectory),
            ("ResourceTable", header => header.ResourceTableDirectory),
            ("ExceptionTable", header => header.ExceptionTableDirectory),
            ("CertificateTable", header => header.CertificateTableDirectory),
            ("BaseRelocationTable", header => header.BaseRelocationTableDirectory),
            ("DebugTable", header => header.DebugTableDirectory),
            ("CopyrightTable", header => header.CopyrightTableDirectory),
            ("GlobalPointerTable", header => header.GlobalPointerTableDirectory),
            ("ThreadLocalStorageTable", header => header.ThreadLocalStorageTableDirectory),
            ("LoadConfigTable", header => header.LoadConfigTableDirectory),
            ("BoundImportTable", header => header.BoundImportTableDirectory),
            ("ImportAddressTable", header => header.ImportAddressTableDirectory),
            ("DelayImportTable", header => header.DelayImportTableDirectory),
            ("CorHeader", header => header.CorHeaderTableDirectory),
        };

        private string _pendingTitle;

        public Mdv(Arguments arguments)
        {
            _arguments = arguments;
            _writer = (arguments.OutputPath != null) ? new StreamWriter(File.Create(arguments.OutputPath), Encoding.UTF8) : Console.Out;
        }

        public void Dispose()
        {
            _writer.Dispose();
        }

        private static string FormatHex(uint value, int width = 8)
        {
            var format = $"X{width}";
            return "0x" + value.ToString(format, CultureInfo.InvariantCulture);
        }

        private static string FormatHex(ulong value, int width = 16)
        {
            var format = $"X{width}";
            return "0x" + value.ToString(format, CultureInfo.InvariantCulture);
        }

        private static string FormatTimeDateStamp(int timeDateStamp)
        {
            uint value = unchecked((uint)timeDateStamp);
            string hex = FormatHex(value);

            if (value == 0)
            {
                return hex;
            }

            try
            {
                var timestamp = DateTimeOffset.FromUnixTimeSeconds(value);
                return $"{hex} ({timestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)} UTC)";
            }
            catch (ArgumentOutOfRangeException)
            {
                return hex;
            }
        }

        private void WriteData(string line, params object[] args)
        {
            if (_pendingTitle != null)
            {
                _writer.WriteLine(_pendingTitle);
                _pendingTitle = null;
            }

            _writer.WriteLine(line, args);
        }

        private static int Main(string[] args)
        {
            var arguments = Arguments.TryParse(args);
            if (arguments == null)
            {
                Console.WriteLine(Arguments.Help);
                return 1;
            }

            using (var mdv = new Mdv(arguments))
            {
                if (arguments.Recursive)
                {
                    return mdv.RunRecursive();
                }
                else
                {
                    return mdv.RunOne();
                }
            }
        }

        private static bool IsPEStream(Stream stream)
        {
            long oldPosition = stream.Position;
            bool result = stream.ReadByte() == 'M' && stream.ReadByte() == 'Z';
            stream.Position = oldPosition;
            return result;
        }

        private static bool IsManagedMetadata(Stream stream)
        {
            long oldPosition = stream.Position;
            bool result = stream.ReadByte() == 'B' && stream.ReadByte() == 'S' && stream.ReadByte() == 'J' && stream.ReadByte() == 'B';
            stream.Position = oldPosition;
            return result;
        }

        private static GenerationData ReadBaseline(string peFilePath, bool embeddedPdb)
        {
            try
            {
                var stream = File.OpenRead(peFilePath);

                if (IsPEStream(stream))
                {
                    var peReader = new PEReader(stream);

                    if (embeddedPdb)
                    {
                        var embeddedEntries = peReader.ReadDebugDirectory().Where(entry => entry.Type == DebugDirectoryEntryType.EmbeddedPortablePdb).ToArray();
                        if (embeddedEntries.Length == 0)
                        {
                            throw new InvalidDataException("No embedded pdb found");
                        }

                        if (embeddedEntries.Length > 1)
                        {
                            throw new InvalidDataException("Multiple entries in Debug Directory Table of type EmbeddedPortablePdb");
                        }

                        var provider = peReader.ReadEmbeddedPortablePdbDebugDirectoryData(embeddedEntries[0]);

                        return new GenerationData(provider, provider.GetMetadataReader(), peReader);
                    }
                    else
                    {
                        return new GenerationData(peReader, peReader.GetMetadataReader(), peReader);
                    }
                }
                else if (IsManagedMetadata(stream))
                {
                    if (embeddedPdb)
                    {
                        throw new InvalidOperationException("File is not PE file");
                    }

                    var mdProvider = MetadataReaderProvider.FromMetadataStream(stream);
                    return new GenerationData(mdProvider, mdProvider.GetMetadataReader());
                }
                else
                {
                    throw new NotSupportedException("File format not supported");
                }

            }
            catch (Exception e)
            {
                Console.WriteLine($"Error reading '{peFilePath}': {e.Message}");
                return null;
            }
        }

        private static GenerationData ReadDelta(string metadataPath, string ilPathOpt)
        {
            byte[] ilDelta;
            try
            {
                ilDelta = (ilPathOpt != null) ? File.ReadAllBytes(ilPathOpt) : null;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error reading '{ilPathOpt}': {e.Message}");
                return null;
            }

            MetadataReaderProvider mdProvider;
            try
            {
                var stream = File.OpenRead(metadataPath);

                if (!IsManagedMetadata(stream))
                {
                    throw new NotSupportedException("File format not supported");
                }

                mdProvider = MetadataReaderProvider.FromMetadataStream(stream);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error reading '{metadataPath}': {e.Message}");
                return null;
            }

            return new GenerationData(mdProvider, mdProvider.GetMetadataReader(), ilDelta: ilDelta);
        }

        private int RunOne()
        {
            var generations = new List<GenerationData>();

            // gen 0:
            var generation = ReadBaseline(_arguments.Path, embeddedPdb: _arguments.DisplayEmbeddedPdb);
            if (generation == null)
            {
                return 1;
            }

            generations.Add(generation);

            Debug.Assert(!_arguments.DisplayEmbeddedPdb || _arguments.EncDeltas.IsEmpty);

            // deltas:
            int i = 1;
            foreach (var (metadataPath, ilPathOpt) in _arguments.EncDeltas)
            {
                generation = ReadDelta(metadataPath, ilPathOpt);
                if (generation == null)
                {
                    return 1;
                }

                generations.Add(generation);
                i++;
            }

            try
            {
                VisualizeGenerations(generations);
            }
            catch (BadImageFormatException e)
            {
                Console.WriteLine("Error reading metadata: " + e.Message);
                return 1;
            }

            return 0;
        }

        private void VisualizeGenerations(List<GenerationData> generations)
        {
            var mdReaders = generations.Select(g => g.MetadataReader).ToArray();

            var options = _arguments.DisplayEmbeddedSource ?
                MetadataVisualizerOptions.EmbeddedSource :
                MetadataVisualizerOptions.None;

            var visualizer = new MetadataVisualizer(mdReaders, _writer, options);

            for (int generationIndex = 0; generationIndex < generations.Count; generationIndex++)
            {
                if (_arguments.SkipGenerations.Contains(generationIndex))
                {
                    continue;
                }

                var generation = generations[generationIndex];
                var mdReader = generation.MetadataReader;

                if (generation.PEReaderOpt != null)
                {
                    if (_arguments.DisplayPEHeaders)
                    {
                        VisualizePEHeaders(generation.PEReaderOpt, _writer);
                    }

                    VisualizeDebugDirectory(generation.PEReaderOpt, _writer);
                    VisualizeReadyToRunDirectory(generation.PEReaderOpt, _writer);
                }

                visualizer.VisualizeHeaders();

                if (generations.Count > 1)
                {
                    _writer.WriteLine(">>>");
                    _writer.WriteLine($">>> Generation {generationIndex}:");
                    _writer.WriteLine(">>>");
                    _writer.WriteLine();
                }

                if (_arguments.DisplayMetadata)
                {
                    visualizer.Visualize(generationIndex);
                }

                if (_arguments.DisplayIL)
                {
                    VisualizeGenerationIL(visualizer, generationIndex, generation, mdReader);
                }

                VisualizeMemberRefs(mdReader);
            }
        }

        private static void VisualizePEHeaders(PEReader peReader, TextWriter writer)
        {
            PEHeaders headers;
            try
            {
                headers = peReader.PEHeaders;
            }
            catch (BadImageFormatException)
            {
                writer.WriteLine("PE Headers:");
                writer.WriteLine("  <bad image>");
                writer.WriteLine();
                return;
            }

            if (headers == null)
            {
                return;
            }

            writer.WriteLine("PE Headers:");

            var coffHeader = headers.CoffHeader;
            writer.WriteLine($"  Machine: {coffHeader.Machine}");
            writer.WriteLine($"  NumberOfSections: {coffHeader.NumberOfSections}");
            writer.WriteLine($"  TimeDateStamp: {FormatTimeDateStamp(coffHeader.TimeDateStamp)}");
            writer.WriteLine($"  PointerToSymbolTable: {FormatHex(unchecked((uint)coffHeader.PointerToSymbolTable))}");
            writer.WriteLine($"  NumberOfSymbols: {coffHeader.NumberOfSymbols}");
            writer.WriteLine($"  SizeOfOptionalHeader: {coffHeader.SizeOfOptionalHeader}");
            writer.WriteLine($"  Characteristics: {FormatHex((uint)coffHeader.Characteristics, 4)} ({coffHeader.Characteristics})");

            var optionalHeader = headers.PEHeader;
            if (optionalHeader != null)
            {
                int addressWidth = optionalHeader.Magic == PEMagic.PE32 ? 8 : 16;

                writer.WriteLine("  OptionalHeader:");
                writer.WriteLine($"    Magic: {optionalHeader.Magic}");
                writer.WriteLine($"    MajorLinkerVersion: {optionalHeader.MajorLinkerVersion}");
                writer.WriteLine($"    MinorLinkerVersion: {optionalHeader.MinorLinkerVersion}");
                writer.WriteLine($"    SizeOfCode: {FormatHex((uint)optionalHeader.SizeOfCode)}");
                writer.WriteLine($"    SizeOfInitializedData: {FormatHex((uint)optionalHeader.SizeOfInitializedData)}");
                writer.WriteLine($"    SizeOfUninitializedData: {FormatHex((uint)optionalHeader.SizeOfUninitializedData)}");
                writer.WriteLine($"    AddressOfEntryPoint: {FormatHex((uint)optionalHeader.AddressOfEntryPoint)}");
                writer.WriteLine($"    BaseOfCode: {FormatHex((uint)optionalHeader.BaseOfCode)}");
                if (optionalHeader.Magic == PEMagic.PE32)
                {
                    writer.WriteLine($"    BaseOfData: {FormatHex((uint)optionalHeader.BaseOfData)}");
                }

                writer.WriteLine($"    ImageBase: {FormatHex(optionalHeader.ImageBase, addressWidth)}");
                writer.WriteLine($"    SectionAlignment: {FormatHex((uint)optionalHeader.SectionAlignment)}");
                writer.WriteLine($"    FileAlignment: {FormatHex((uint)optionalHeader.FileAlignment)}");
                writer.WriteLine($"    OperatingSystemVersion: {optionalHeader.MajorOperatingSystemVersion}.{optionalHeader.MinorOperatingSystemVersion}");
                writer.WriteLine($"    ImageVersion: {optionalHeader.MajorImageVersion}.{optionalHeader.MinorImageVersion}");
                writer.WriteLine($"    SubsystemVersion: {optionalHeader.MajorSubsystemVersion}.{optionalHeader.MinorSubsystemVersion}");
                writer.WriteLine($"    SizeOfImage: {FormatHex((uint)optionalHeader.SizeOfImage)}");
                writer.WriteLine($"    SizeOfHeaders: {FormatHex((uint)optionalHeader.SizeOfHeaders)}");
                writer.WriteLine($"    CheckSum: {FormatHex((uint)optionalHeader.CheckSum)}");
                writer.WriteLine($"    Subsystem: {optionalHeader.Subsystem}");
                writer.WriteLine($"    DllCharacteristics: {FormatHex((uint)optionalHeader.DllCharacteristics, 4)} ({optionalHeader.DllCharacteristics})");
                writer.WriteLine($"    SizeOfStackReserve: {FormatHex(optionalHeader.SizeOfStackReserve, addressWidth)}");
                writer.WriteLine($"    SizeOfStackCommit: {FormatHex(optionalHeader.SizeOfStackCommit, addressWidth)}");
                writer.WriteLine($"    SizeOfHeapReserve: {FormatHex(optionalHeader.SizeOfHeapReserve, addressWidth)}");
                writer.WriteLine($"    SizeOfHeapCommit: {FormatHex(optionalHeader.SizeOfHeapCommit, addressWidth)}");
                writer.WriteLine($"    NumberOfRvaAndSizes: {optionalHeader.NumberOfRvaAndSizes}");

                if (optionalHeader.NumberOfRvaAndSizes > 0)
                {
                    writer.WriteLine("    DataDirectories:");
                    foreach (var (name, accessor) in s_peDataDirectories)
                    {
                        DirectoryEntry entry = accessor(optionalHeader);
                        if (entry.RelativeVirtualAddress == 0 && entry.Size == 0)
                        {
                            continue;
                        }

                        writer.WriteLine($"      {name}: RVA={FormatHex((uint)entry.RelativeVirtualAddress)} Size={FormatHex((uint)entry.Size)}");
                    }
                }
            }

            var sectionHeaders = headers.SectionHeaders;
            if (sectionHeaders.Length > 0)
            {
                writer.WriteLine("  Sections:");
                foreach (var section in sectionHeaders)
                {
                    writer.WriteLine($"    {section.Name}:");
                    writer.WriteLine($"      VirtualSize: {FormatHex(unchecked((uint)section.VirtualSize))}");
                    writer.WriteLine($"      VirtualAddress: {FormatHex(unchecked((uint)section.VirtualAddress))}");
                    writer.WriteLine($"      SizeOfRawData: {FormatHex(unchecked((uint)section.SizeOfRawData))}");
                    writer.WriteLine($"      PointerToRawData: {FormatHex(unchecked((uint)section.PointerToRawData))}");
                    writer.WriteLine($"      PointerToRelocations: {FormatHex(unchecked((uint)section.PointerToRelocations))}");
                    writer.WriteLine($"      PointerToLineNumbers: {FormatHex(unchecked((uint)section.PointerToLineNumbers))}");
                    writer.WriteLine($"      NumberOfRelocations: {section.NumberOfRelocations}");
                    writer.WriteLine($"      NumberOfLineNumbers: {section.NumberOfLineNumbers}");
                    writer.WriteLine($"      Characteristics: {FormatHex((uint)section.SectionCharacteristics)} ({section.SectionCharacteristics})");
                }
            }

            writer.WriteLine();
        }

        private static void VisualizeDebugDirectory(PEReader peReader, TextWriter writer)
        {
            var entries = peReader.ReadDebugDirectory();

            if (entries.Length == 0)
            {
                return;
            }

            writer.WriteLine("Debug Directory:");
            foreach (var entry in entries)
            {
                writer.WriteLine($"  {entry.Type} stamp=0x{entry.Stamp:X8}, version=(0x{entry.MajorVersion:X4}, 0x{entry.MinorVersion:X4}), size={entry.DataSize}");

                try
                {
                    switch (entry.Type)
                    {
                        case DebugDirectoryEntryType.CodeView:
                            var codeView = peReader.ReadCodeViewDebugDirectoryData(entry);
                            writer.WriteLine($"    path='{codeView.Path}', guid={{{codeView.Guid}}}, age={codeView.Age}");
                            break;
                    }
                }
                catch (BadImageFormatException)
                {
                    writer.WriteLine("<bad data>");
                }
            }

            writer.WriteLine();
        }
        
        /// <summary>
        /// Based on https://github.com/dotnet/coreclr/blob/master/src/inc/pedecoder.h IMAGE_FILE_MACHINE_NATIVE_OS_OVERRIDE
        /// </summary>
        private enum OperatingSystem
        {
            Apple = 0x4644,
            FreeBSD = 0xADC4,
            Linux = 0x7B79,
            NetBSD = 0x1993,
            Windows = 0,
            Unknown = -1
        }

        private static (Machine, OperatingSystem) GetTargetMachineAndOperatingSystem(Machine machine)
        {
            foreach (var os in Enum.GetValues<OperatingSystem>())
            {
                var actualMachine = (Machine)((uint)machine ^ (uint)os);
                if (Enum.IsDefined(actualMachine))
                {
                    return (machine, os);
                }
            }

            return (machine, OperatingSystem.Unknown);
        }

        private static void VisualizeReadyToRunDirectory(PEReader peReader, TextWriter writer)
        {
            var managedNativeDir = peReader.PEHeaders.CorHeader.ManagedNativeHeaderDirectory;
            if (managedNativeDir.Size == 0)
            {
                return;
            }

            var (machine, operatingSystem) = GetTargetMachineAndOperatingSystem(peReader.PEHeaders.CoffHeader.Machine);
            var architecture = machine switch
            {
                Machine.I386 => Architecture.X86,
                Machine.Amd64 => Architecture.X64,
                Machine.Arm or Machine.Thumb or Machine.ArmThumb2 => Architecture.Arm,
                Machine.Arm64 => Architecture.Arm64,
                _ => (Architecture?)null,
            };

            writer.WriteLine("Ready2Run image:");
            writer.WriteLine($"  Target architecture: {architecture}, operating system: {operatingSystem}, machine code: {machine}.");
            writer.WriteLine();
        }

        private static unsafe void VisualizeGenerationIL(MetadataVisualizer visualizer, int generationIndex, GenerationData generation, MetadataReader mdReader)
        {
            try
            {
                if (generation.PEReaderOpt != null)
                {
                    foreach (var methodHandle in mdReader.MethodDefinitions)
                    {
                        visualizer.VisualizeMethodBody(methodHandle, rva => generation.PEReaderOpt.GetMethodBody(rva));
                    }
                }
                else if (generation.ILDeltaOpt != null)
                {
                    fixed (byte* deltaILPtr = generation.ILDeltaOpt)
                    {
                        foreach (var generationHandle in mdReader.MethodDefinitions)
                        {
                            var method = mdReader.GetMethodDefinition(generationHandle);
                            var rva = method.RelativeVirtualAddress;
                            if (rva != 0)
                            {
                                var body = MethodBodyBlock.Create(new BlobReader(deltaILPtr + rva, generation.ILDeltaOpt.Length - rva));

                                visualizer.VisualizeMethodBody(body, generationHandle, generationIndex);
                            }
                        }
                    }
                }
                else
                {
                    visualizer.WriteLine("<IL not available>");
                }
            }
            catch (BadImageFormatException)
            {
                visualizer.WriteLine("<bad metadata>");
            }
        }

        private static readonly string[] s_PEExtensions = new[] { "*.dll", "*.exe", "*.netmodule", "*.winmd" };

        private static IEnumerable<string> GetAllBinaries(string dir)
        {
            foreach (var subdir in Directory.GetDirectories(dir))
            {
                foreach (var file in GetAllBinaries(subdir))
                {
                    yield return file;
                }
            }

            foreach (var file in from extension in s_PEExtensions
                                 from file in Directory.GetFiles(dir, extension)
                                 select file)
            {
                yield return file;
            }
        }

        private void VisualizeStatistics(MetadataReader mdReader)
        {
            if (!_arguments.DisplayStatistics)
            {
                return;
            }

            WriteData("> method definitions: {0}, {1:F1}% with bodies",
                mdReader.MethodDefinitions.Count,
                100 * ((double)mdReader.MethodDefinitions.Count(handle => mdReader.GetMethodDefinition(handle).RelativeVirtualAddress != 0) / mdReader.MethodDefinitions.Count));
        }

        private void VisualizeAssemblyReferences(MetadataReader mdReader)
        {
            if (!_arguments.DisplayAssemblyReferences)
            {
                return;
            }

            foreach (var handle in mdReader.AssemblyReferences)
            {
                var ar = mdReader.GetAssemblyReference(handle);

                WriteData("{0}, Version={1}, PKT={2}",
                    mdReader.GetString(ar.Name),
                    ar.Version,
                    BitConverter.ToString(mdReader.GetBlobBytes(ar.PublicKeyOrToken)));
            }
        }

        private void VisualizeMemberRefs(MetadataReader mdReader)
        {
            if (!_arguments.FindRefs.Any())
            {
                return;
            }

            var memberRefs = new HashSet<MemberRefKey>(
                from arg in _arguments.FindRefs
                let split = arg.Split(':')
                where split.Length == 3
                select new MemberRefKey(split[0].Trim(), split[1].Trim(), split[2].Trim()));

            foreach (var handle in mdReader.MemberReferences)
            {
                var memberRef = mdReader.GetMemberReference(handle);

                if (memberRef.Parent.Kind != HandleKind.TypeReference)
                {
                    continue;
                }

                var typeRef = mdReader.GetTypeReference((TypeReferenceHandle)memberRef.Parent);
                if (typeRef.ResolutionScope.Kind != HandleKind.AssemblyReference)
                {
                    // TODO: handle nested types
                    continue;
                }

                var assemblyRef = mdReader.GetAssemblyReference((AssemblyReferenceHandle)typeRef.ResolutionScope);

                var key = new MemberRefKey(
                    assemblyNameOpt: mdReader.GetString(assemblyRef.Name),
                    assemblyVersionOpt: assemblyRef.Version,
                    @namespace: mdReader.GetString(typeRef.Namespace),
                    typeName: mdReader.GetString(typeRef.Name),
                    memberName: mdReader.GetString(memberRef.Name)
                );

                if (memberRefs.Contains(key))
                {
                    WriteData($"0x{MetadataTokens.GetToken(handle):X8}->0x{MetadataTokens.GetToken(memberRef.Parent):X8}:" + $" {key.ToString()}");
                }
            }
        }

        private struct MemberRefKey : IEquatable<MemberRefKey>
        {
            public readonly string AssemblyNameOpt;
            public readonly Version AssemblyVersionOpt;
            public readonly string Namespace;
            public readonly string TypeName;
            public readonly string MemberName;

            public MemberRefKey(string assemblyName, string qualifiedTypeName, string memberName)
            {
                if (assemblyName.Length > 0)
                {
                    var an = new AssemblyName(assemblyName);
                    AssemblyNameOpt = an.Name;
                    AssemblyVersionOpt = an.Version;
                }
                else
                {
                    AssemblyNameOpt = null;
                    AssemblyVersionOpt = null;
                }

                var lastDot = qualifiedTypeName.LastIndexOf('.');
                Namespace = (lastDot >= 0) ? qualifiedTypeName.Substring(0, lastDot) : "";
                TypeName = (lastDot >= 0) ? qualifiedTypeName.Substring(lastDot + 1) : "";

                MemberName = memberName;
            }

            public MemberRefKey(
                string assemblyNameOpt,
                Version assemblyVersionOpt,
                string @namespace,
                string typeName,
                string memberName)
            {
                AssemblyNameOpt = assemblyNameOpt;
                AssemblyVersionOpt = assemblyVersionOpt;
                Namespace = @namespace;
                TypeName = typeName;
                MemberName = memberName;
            }

            public override bool Equals(object obj)
            {
                return obj is MemberRefKey && Equals((MemberRefKey)obj);
            }

            public override int GetHashCode()
            {
                // don't include assembly name/version
                return Hash.Combine(Namespace,
                       Hash.Combine(TypeName,
                       Hash.Combine(MemberName, 0)));
            }

            public bool Equals(MemberRefKey other)
            {
                return (this.AssemblyNameOpt == null || other.AssemblyNameOpt == null || this.AssemblyNameOpt.Equals(other.AssemblyNameOpt, StringComparison.OrdinalIgnoreCase)) &&
                       (this.AssemblyVersionOpt == null || other.AssemblyVersionOpt == null || this.AssemblyVersionOpt.Equals(other.AssemblyVersionOpt)) &&
                       this.Namespace.Equals(other.Namespace) &&
                       this.TypeName.Equals(other.TypeName) &&
                       this.MemberName.Equals(other.MemberName);
            }

            public override string ToString()
            {
                return (AssemblyNameOpt != null ? $"{AssemblyNameOpt}, Version={AssemblyVersionOpt}" : "") +
                       $":{Namespace}{(Namespace.Length > 0 ? "." : "")}{TypeName}:{MemberName}";
            }
        }

        private int RunRecursive()
        {
            bool hasError = false;

            foreach (var file in GetAllBinaries(_arguments.Path))
            {
                using (var peReader = new PEReader(File.OpenRead(file)))
                {
                    try
                    {
                        if (!peReader.HasMetadata)
                        {
                            continue;
                        }
                    }
                    catch (BadImageFormatException e)
                    {
                        _writer.WriteLine("{0}: {1}", file, e.Message);
                        hasError = true;
                        continue;
                    }

                    _pendingTitle = file;

                    try
                    {
                        var mdReader = peReader.GetMetadataReader();

                        VisualizeAssemblyReferences(mdReader);
                        VisualizeStatistics(mdReader);
                        VisualizeMemberRefs(mdReader);
                    }
                    catch (BadImageFormatException e)
                    {
                        WriteData("ERROR: {0}", e.Message);
                        hasError = true;
                        continue;
                    }

                    if (_pendingTitle == null)
                    {
                        _writer.WriteLine();
                    }

                    _pendingTitle = null;
                }
            }

            return hasError ? 1 : 0;
        }
    }
}
