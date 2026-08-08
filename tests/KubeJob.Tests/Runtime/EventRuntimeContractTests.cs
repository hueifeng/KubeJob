using FluentAssertions;
using KubeJob.Core.Events;

namespace KubeJob.Tests.Runtime;

public sealed class EventRuntimeContractTests
{
    [Fact]
    public void Event_idempotency_key_is_rejected_until_subscription_deduplication_exists()
    {
        var options = new EventPublishOptions
        {
            IdempotencyKey = "order-created:42"
        };

        var act = options.Validate;

        act.Should().Throw<NotSupportedException>()
            .WithMessage("*Inbox/deduplication*");
    }
}
