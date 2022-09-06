// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

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
                => typeCode switch
                {
                    PrimitiveTypeCode.Boolean => "bool",
                    PrimitiveTypeCode.Byte => "uint8",
                    PrimitiveTypeCode.Char => "char",
                    PrimitiveTypeCode.Double => "float64",
                    PrimitiveTypeCode.Int16 => "int16",
                    PrimitiveTypeCode.Int32 => "int32",
                    PrimitiveTypeCode.Int64 => "int64",
                    PrimitiveTypeCode.IntPtr => "native int",
                    PrimitiveTypeCode.Object => "object",
                    PrimitiveTypeCode.SByte => "int8",
                    PrimitiveTypeCode.Single => "float32",
                    PrimitiveTypeCode.String => "string",
                    PrimitiveTypeCode.TypedReference => "typedref",
                    PrimitiveTypeCode.UInt16 => "uint16",
                    PrimitiveTypeCode.UInt32 => "uint32",
                    PrimitiveTypeCode.UInt64 => "uint64",
                    PrimitiveTypeCode.UIntPtr => "native uint",
                    PrimitiveTypeCode.Void => "void",
                    _ => "<bad metadata>",
                };

            public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind = 0)
                => _visualizer.QualifiedTypeDefinitionName(handle);

            public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind = 0)
                => _visualizer.QualifiedTypeReferenceName(handle);

            public string GetTypeFromSpecification(MetadataReader reader, object genericContext, TypeSpecificationHandle handle, byte rawTypeKind = 0)
                => _visualizer.QualifiedTypeSpecificationName(handle);

            public string GetSZArrayType(string elementType)
                => elementType + "[]";

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

            public string GetModifiedType(string modifierType, string unmodifiedType, bool isRequired)
                => unmodifiedType + (isRequired ? " modreq(" : " modopt(") + modifierType + ")";

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
