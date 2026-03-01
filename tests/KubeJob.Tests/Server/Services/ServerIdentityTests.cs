using FluentAssertions;
using KubeJob.Server.Services;
using Xunit;

namespace KubeJob.Tests.Server.Services
{
    public class ServerIdentityTests
    {
        [Fact]
        public void DefaultServerIdentity_ShouldGenerateUniqueId()
        {
            // Arrange
            var identity1 = new DefaultServerIdentity();
            var identity2 = new DefaultServerIdentity();

            // Assert
            identity1.ServerId.Should().NotBeNullOrEmpty();
            identity2.ServerId.Should().NotBeNullOrEmpty();
            identity1.ServerId.Should().NotBe(identity2.ServerId);
            
            // Length of Guid.ToString("N") is 32
            identity1.ServerId.Length.Should().Be(32);
        }
    }
}