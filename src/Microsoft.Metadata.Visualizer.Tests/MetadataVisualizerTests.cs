// Copyright (c) Microsoft.  All Rights Reserved.  Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

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
