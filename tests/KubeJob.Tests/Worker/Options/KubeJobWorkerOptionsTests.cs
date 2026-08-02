using System;
using FluentAssertions;
using KubeJob.Worker.Options;
using Xunit;

namespace KubeJob.Tests.Worker.Options
{
    public class KubeJobWorkerOptionsTests
    {
        [Fact]
        public void KubeJobWorkerOptions_ShouldHaveSensibleDefaults()
        {
            // Arrange
            var options = new KubeJobWorkerOptions();

            // Assert
            options.ServerEndpoint.Should().Be("http://localhost:5000");
            options.MaxConcurrentJobs.Should().Be(64);
            options.WorkerId.Should().Be(Environment.MachineName);
            options.Labels.Should().NotBeNull();
            options.Labels.Should().BeEmpty();
        }
    }
}