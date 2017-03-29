// Copyright (c) Microsoft.  All Rights Reserved.  Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Text;

namespace Microsoft.Metadata.Tools
{
    partial class MetadataVisualizer
    {
        // Test implementation of ISignatureTypeProvider<TType, TGenericContext> that uses strings in ilasm syntax as TType.
        // A real provider in any sort of perf constraints would not want to allocate strings freely like this, but it keeps test code simple.
        internal sealed class SignatureVisualizer : ISignatureTypeProvider<string, object>
        {
            private readonly MetadataVisualizer _visualizer;

            public SignatureVisualizer(MetadataVisualizer visualizer)
            {
                _visualizer = visualizer;
            }

            public string GetPrimitiveType(PrimitiveTypeCode typeCode)
            {
                switch (typeCode)
                {
                    case PrimitiveTypeCode.Boolean: return "bool";
                    case PrimitiveTypeCode.Byte: return "uint8";
                    case PrimitiveTypeCode.Char: return "char";
                    case PrimitiveTypeCode.Double: return "float64";
                    case PrimitiveTypeCode.Int16: return "int16";
                    case PrimitiveTypeCode.Int32: return "int32";
                    case PrimitiveTypeCode.Int64: return "int64";
                    case PrimitiveTypeCode.IntPtr: return "native int";
                    case PrimitiveTypeCode.Object: return "object";
                    case PrimitiveTypeCode.SByte: return "int8";
                    case PrimitiveTypeCode.Single: return "float32";
                    case PrimitiveTypeCode.String: return "string";
                    case PrimitiveTypeCode.TypedReference: return "typedref";
                    case PrimitiveTypeCode.UInt16: return "uint16";
                    case PrimitiveTypeCode.UInt32: return "uint32";
                    case PrimitiveTypeCode.UInt64: return "uint64";
                    case PrimitiveTypeCode.UIntPtr: return "native uint";
                    case PrimitiveTypeCode.Void: return "void";
                    default: return "<bad metadata>";
                }
            }

            private string RowId(EntityHandle handle) => _visualizer.RowId(handle);

            public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind = 0) =>
                $"typedef{RowId(handle)}";

            public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind = 0) =>
                $"typeref{RowId(handle)}";

            public string GetTypeFromSpecification(MetadataReader reader, object genericContext, TypeSpecificationHandle handle, byte rawTypeKind = 0) =>
                $"typespec{RowId(handle)}";

            public string GetSZArrayType(string elementType) =>
                elementType + "[]";

            public string GetPointerType(string elementType)
                => elementType + "*";

            public string GetByReferenceType(string elementType)
                => elementType + "&";

            public string GetGenericMethodParameter(object genericContext, int index)
                => "!!" + index;

            public string GetGenericTypeParameter(object genericContext, int index)
                => "!" + index;

            public string GetPinnedType(string elementType)
                => elementType + " pinned";

            public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments)
                => genericType + "<" + string.Join(",", typeArguments) + ">";

            public string GetModifiedType(string modifierType, string unmodifiedType, bool isRequired) =>
                unmodifiedType + (isRequired ? " modreq(" : " modopt(") + modifierType + ")";

            public string GetArrayType(string elementType, ArrayShape shape)
            {
                var builder = new StringBuilder();

                builder.Append(elementType);
                builder.Append('[');

                for (int i = 0; i < shape.Rank; i++)
                {
                    int lowerBound = 0;

                    if (i < shape.LowerBounds.Length)
                    {
                        lowerBound = shape.LowerBounds[i];
                        builder.Append(lowerBound);
                    }

                    builder.Append("...");

                    if (i < shape.Sizes.Length)
                    {
                        builder.Append(lowerBound + shape.Sizes[i] - 1);
                    }

                    if (i < shape.Rank - 1)
                    {
                        builder.Append(',');
                    }
                }

                builder.Append(']');
                return builder.ToString();
            }

            public string GetFunctionPointerType(MethodSignature<string> signature)
                => $"methodptr({MethodSignature(signature)})";
        }
    }
}
