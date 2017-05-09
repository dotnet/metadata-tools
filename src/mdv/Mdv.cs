// Copyright (c) Microsoft.  All Rights Reserved.  Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
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

        private string _pendingTitle;

        public Mdv(Arguments arguments)
        {
            _arguments = arguments;
            _writer = (arguments.OutputPath != null) ? new StreamWriter(File.OpenWrite(arguments.OutputPath), Encoding.UTF8) : Console.Out;
        }

        public void Dispose()
        {
            _writer.Dispose();
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

            Debug.Assert(!_arguments.DisplayEmbeddedPdb || _arguments.EncDeltas.Count == 0);

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
            var visualizer = new MetadataVisualizer(mdReaders, _writer);

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
                    VisualizeDebugDirectory(generation.PEReaderOpt, _writer);
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