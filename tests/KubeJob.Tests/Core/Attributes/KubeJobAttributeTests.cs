using System;
using System.Linq;
using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Enums;
using Xunit;

namespace KubeJob.Tests.Core.Attributes
{
    public class KubeJobAttributeTests
    {
        [Fact]
        public void KubeJobAttribute_Constructor_ShouldSetNameAndDefaultValues()
        {
            // Arrange & Act
            var attribute = new KubeJobAttribute("TestJob");

            // Assert
            attribute.Name.Should().Be("TestJob");
            attribute.Cron.Should().BeEmpty();
            attribute.ExecuteModel.Should().Be(ExecuteModel.Standalone);
            attribute.TotalShards.Should().Be(1);
            attribute.TimeoutSeconds.Should().Be(300);
            attribute.MaxRetries.Should().Be(0);
        }

        [Fact]
        public void NodeSelectorAttribute_Constructor_ShouldSetKeyAndValue()
        {
            // Arrange & Act
            var attribute = new NodeSelectorAttribute("env", "prod");

            // Assert
            attribute.Key.Should().Be("env");
            attribute.Value.Should().Be("prod");
        }

        [KubeJob("MyAnnotatedJob", Cron = "0 0 * * *", ExecuteModel = ExecuteModel.Broadcast, TotalShards = 5, TimeoutSeconds = 60, MaxRetries = 3)]
        [NodeSelector("region", "us-east")]
        [NodeSelector("tier", "backend")]
        private class DummyJob
        {
        }

        [Fact]
        public void Reflection_ShouldReadAttributesCorrectly()
        {
            // Arrange
            var type = typeof(DummyJob);

            // Act
            var jobAttr = (KubeJobAttribute)Attribute.GetCustomAttribute(type, typeof(KubeJobAttribute))!;
            var selectors = Attribute.GetCustomAttributes(type, typeof(NodeSelectorAttribute))
                                     .Cast<NodeSelectorAttribute>()
                                     .ToList();

            // Assert
            jobAttr.Should().NotBeNull();
            jobAttr.Name.Should().Be("MyAnnotatedJob");
            jobAttr.Cron.Should().Be("0 0 * * *");
            jobAttr.ExecuteModel.Should().Be(ExecuteModel.Broadcast);
            jobAttr.TotalShards.Should().Be(5);
            jobAttr.TimeoutSeconds.Should().Be(60);
            jobAttr.MaxRetries.Should().Be(3);

            selectors.Should().HaveCount(2);
            selectors.Should().Contain(s => s.Key == "region" && s.Value == "us-east");
            selectors.Should().Contain(s => s.Key == "tier" && s.Value == "backend");
        }
    }
}