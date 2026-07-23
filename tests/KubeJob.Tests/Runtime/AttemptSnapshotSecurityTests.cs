using System.Text.Json;
using FluentAssertions;
using KubeJob.Core.Client;
using KubeJob.Core.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class AttemptSnapshotSecurityTests
{
    [Fact]
    public void User_facing_attempt_snapshot_has_no_lease_token_property()
    {
        typeof(JobAttemptSnapshot).GetProperties()
            .Select(property => property.Name)
            .Should().NotContain(name =>
                name.Contains("LeaseToken", StringComparison.OrdinalIgnoreCase)
                || name.Contains("FencingToken", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Serialized_attempt_snapshot_does_not_contain_fencing_credentials()
    {
        var snapshot = new JobAttemptSnapshot(
            "attempt-1",
            1,
            "worker-1",
            "session-1",
            4,
            JobAttemptPhase.Running,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddSeconds(30),
            null,
            null,
            null);

        var json = JsonSerializer.Serialize(snapshot);

        json.Should().NotContain("token", StringComparison.OrdinalIgnoreCase);
    }
}
