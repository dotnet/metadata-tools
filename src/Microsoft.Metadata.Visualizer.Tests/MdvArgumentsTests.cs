// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the License.txt file in the project root for more information.

﻿#if !NETFRAMEWORK

using System;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Microsoft.Metadata.Tools.UnitTests
{
    public class MdvArgumentsTests
    {
        private static Type GetArgumentsType()
        {
            var assembly = AppDomain.CurrentDomain
                .GetAssemblies()
                .FirstOrDefault(a => string.Equals(a.GetName().Name, "mdv", StringComparison.OrdinalIgnoreCase))
                ?? Assembly.Load("mdv");

            return assembly.GetType("Arguments", throwOnError: true);
        }

        private static object InvokeTryParse(Type argumentsType, params string[] args)
        {
            var tryParse = argumentsType.GetMethod("TryParse", BindingFlags.Public | BindingFlags.Static);
            return tryParse.Invoke(null, new object[] { args });
        }

        private static bool GetDisplayPeHeaders(object arguments, Type argumentsType)
        {
            var property = argumentsType.GetProperty("DisplayPEHeaders", BindingFlags.Public | BindingFlags.Instance);
            return (bool)property.GetValue(arguments);
        }

        [Fact]
        public void DisplayPeHeaders_DefaultsToFalse()
        {
            var argumentsType = GetArgumentsType();
            var arguments = InvokeTryParse(argumentsType, "input.dll");

            Assert.NotNull(arguments);
            Assert.False(GetDisplayPeHeaders(arguments, argumentsType));
        }

        [Theory]
        [InlineData("/peHeaders")]
        [InlineData("/peHeaders+")]
        public void DisplayPeHeaders_EnabledViaSwitch(string switchValue)
        {
            var argumentsType = GetArgumentsType();
            var arguments = InvokeTryParse(argumentsType, "input.dll", switchValue);

            Assert.NotNull(arguments);
            Assert.True(GetDisplayPeHeaders(arguments, argumentsType));
        }

        [Fact]
        public void DisplayPeHeaders_DisabledWithMinusSwitch()
        {
            var argumentsType = GetArgumentsType();
            var arguments = InvokeTryParse(argumentsType, "input.dll", "/peHeaders+", "/peHeaders-");

            Assert.NotNull(arguments);
            Assert.False(GetDisplayPeHeaders(arguments, argumentsType));
        }
    }
}

#endif
