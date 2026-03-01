using System;
using System.Threading.Tasks;
using FluentAssertions;
using KubeJob.Server.Data;
using Xunit;

namespace KubeJob.Tests.Server.Data
{
    public class InMemoryLockProviderTests
    {
        private readonly InMemoryLockProvider _provider;
        private readonly string _lockName = "test-lock";
        private readonly string _holderId = "server-1";
        private readonly TimeSpan _expiry = TimeSpan.FromSeconds(5);

        public InMemoryLockProviderTests()
        {
            _provider = new InMemoryLockProvider();
        }

        [Fact]
        public async Task TryAcquireLockAsync_WhenLockIsAvailable_ShouldReturnTrue()
        {
            // Act
            var result = await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task TryAcquireLockAsync_WhenLockIsHeldBySelf_ShouldReturnTrueAndRenew()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Act
            var result = await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task TryAcquireLockAsync_WhenLockIsHeldByOther_ShouldReturnFalse()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, "other-server", _expiry);

            // Act
            var result = await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Assert
            result.Should().BeFalse();
        }

        [Fact]
        public async Task TryAcquireLockAsync_WhenLockIsExpired_ShouldReturnTrue()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, "other-server", TimeSpan.FromMilliseconds(10));
            await Task.Delay(50); // wait for it to expire

            // Act
            var result = await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task RenewLockAsync_WhenLockIsHeldBySelf_ShouldReturnTrue()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Act
            var result = await _provider.RenewLockAsync(_lockName, _holderId, TimeSpan.FromSeconds(10));

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task RenewLockAsync_WhenLockIsHeldByOther_ShouldReturnFalse()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, "other-server", _expiry);

            // Act
            var result = await _provider.RenewLockAsync(_lockName, _holderId, _expiry);

            // Assert
            result.Should().BeFalse();
        }

        [Fact]
        public async Task ReleaseLockAsync_WhenLockIsHeldBySelf_ShouldRelease()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, _holderId, _expiry);

            // Act
            await _provider.ReleaseLockAsync(_lockName, _holderId);
            var acquireAgain = await _provider.TryAcquireLockAsync(_lockName, "other-server", _expiry);

            // Assert
            acquireAgain.Should().BeTrue();
        }

        [Fact]
        public async Task ReleaseLockAsync_WhenLockIsHeldByOther_ShouldNotRelease()
        {
            // Arrange
            await _provider.TryAcquireLockAsync(_lockName, "other-server", _expiry);

            // Act
            await _provider.ReleaseLockAsync(_lockName, _holderId);
            var acquireAgain = await _provider.TryAcquireLockAsync(_lockName, "yet-another", _expiry);

            // Assert
            acquireAgain.Should().BeFalse();
        }
    }
}