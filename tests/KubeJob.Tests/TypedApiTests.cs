using System.Reflection;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Jobs;

namespace KubeJob.Tests;

public sealed class TypedApiTests
{
    [Fact]
    public void JobKey_preserves_stable_identifier()
    {
        var key = new JobKey<string>("mail.send");

        Assert.Equal("mail.send", key.Value);
        Assert.Equal("mail.send", key.ToString());
        Assert.False(key.IsEmpty);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void JobKey_rejects_empty_identifier(string value)
    {
        Assert.Throws<ArgumentException>(() => new JobKey<string>(value));
    }

    [Fact]
    public void Execution_context_does_not_expose_service_provider()
    {
        PropertyInfo? property = typeof(JobExecutionContext)
            .GetProperty("ServiceProvider", BindingFlags.Public | BindingFlags.Instance);

        Assert.NotNull(property);
    }

    [Theory]
    [InlineData("Mail.Send")]
    [InlineData("mail_send")]
    [InlineData("mail/send")]
    public void Logical_queue_rejects_non_canonical_names(string queue)
    {
        Assert.Throws<ArgumentException>(() => new JobEnqueueOptions { Queue = queue }.ResolveQueue("mail.send"));
    }

    [Fact]
    public void Logical_queue_keeps_the_literal_business_name()
    {
        var queue = new JobEnqueueOptions { Queue = " mail.send " }.ResolveQueue("ignored");

        Assert.Equal("mail.send", queue);
    }
}
