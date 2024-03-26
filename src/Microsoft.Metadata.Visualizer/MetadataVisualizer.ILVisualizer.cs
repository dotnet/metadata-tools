// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

#nullable disable

using System.Reflection.Emit;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;

namespace Microsoft.Metadata.Tools
{
    partial class MetadataVisualizer
    {
        private sealed class ILVisualizer : Tools.ILVisualizer
        {
            private readonly MetadataVisualizer _metadataVisualizer;
            private readonly TypeDefinitionHandle _scope;

            public ILVisualizer(MetadataVisualizer metadataVisualizer, TypeDefinitionHandle scope)
            {
                _metadataVisualizer = metadataVisualizer;
                _scope = scope;
            }

            public override string VisualizeSymbol(uint token, OperandType operandType)
            {
                var handle = MetadataTokens.EntityHandle((int)token);
                var tokenString = _metadataVisualizer.Token(handle, displayTable: false);
                var name = _metadataVisualizer.QualifiedName(handle, _scope);
                return (name != null) ? $"'{StringUtilities.EscapeNonPrintableCharacters(name)}' ({tokenString})" : tokenString;
            }

            public override string VisualizeUserString(uint token)
            {
                var handle = (UserStringHandle)MetadataTokens.Handle((int)token);
                return '"' + StringUtilities.EscapeNonPrintableCharacters(_metadataVisualizer.GetString(handle)) + '"';
            }
        }
    }
}
