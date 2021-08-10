// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection.Metadata;
using Xunit;

namespace Microsoft.Metadata.Tools.UnitTests
{
    public class MetadataVisualizerTests
    {
        [Fact]
        public void Ctor()
        {
            Assert.Throws<ArgumentNullException>(() => new MetadataVisualizer(default(MetadataReader), new StringWriter()));
            Assert.Throws<ArgumentNullException>(() => new MetadataVisualizer(default(List<MetadataReader>), new StringWriter()));
        }
    }
}
