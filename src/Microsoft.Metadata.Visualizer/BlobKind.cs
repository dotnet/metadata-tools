// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

namespace Microsoft.Metadata.Tools;

public enum BlobKind
{
    None,
    Key,
    FileHash,

    MethodSignature,
    FieldSignature,
    MemberRefSignature,
    StandAloneSignature,

    TypeSpec,
    MethodSpec,

    ConstantValue,
    Marshalling,
    PermissionSet,
    CustomAttribute,

    DocumentName,
    DocumentHash,
    SequencePoints,
    Imports,
    ImportAlias,
    ImportNamespace,
    LocalConstantSignature,
    CustomDebugInformation
}
