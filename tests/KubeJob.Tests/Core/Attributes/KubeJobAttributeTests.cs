using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;

namespace KubeJob.Tests.Core.Attributes;

public sealed class KubeJobAttributeTests
{
    [Fact]
    public void Constructor_sets_trimmed_stable_key()
    {
        var attribute = new KubeJobAttribute("  mail.send  ");

        attribute.Key.Should().Be("mail.send");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_rejects_empty_key(string key)
    {
        var act = () => new KubeJobAttribute(key);

        act.Should().Throw<ArgumentException>();
    }

    private sealed record DummyPayload(string Value);

    [KubeJob("report.generate")]
    private sealed class DummyJob : IKubeJob<DummyPayload>
    {
        public ValueTask ExecuteAsync(
            DummyPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    [Fact]
    public void Reflection_reads_the_same_stable_key_used_by_the_generator()
    {
        var attribute = typeof(DummyJob)
            .GetCustomAttributes(typeof(KubeJobAttribute), inherit: false)
            .Cast<KubeJobAttribute>()
            .Single();

        attribute.Key.Should().Be("report.generate");
    }
}
