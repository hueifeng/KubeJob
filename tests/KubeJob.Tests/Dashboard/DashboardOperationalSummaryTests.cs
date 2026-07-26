using FluentAssertions;
using KubeJob.Server.Dashboard;
using KubeJob.Server.Runtime;

namespace KubeJob.Tests.Dashboard;

public sealed class DashboardOperationalSummaryTests
{
    [Fact]
    public void Delayed_pending_work_is_not_reported_as_a_capacity_outage()
    {
        var summary = DashboardOperationalSummary.Create(Overview(
            pendingRuns: 1,
            readyWorkers: 0,
            availableSlots: 0,
            queues: new[] { new DashboardQueueSummary("default", 1, 0, null) }));

        summary.WaitingWithoutCapacity.Should().BeFalse();
        summary.Message.Title.Should().NotContain("no worker is ready");
    }

    [Fact]
    public void Outbox_only_activity_is_not_shown_as_idle_or_an_operator_alert()
    {
        var summary = DashboardOperationalSummary.Create(Overview(pendingOutboxMessages: 1));

        summary.IsIdle.Should().BeFalse();
        summary.HasAttention.Should().BeFalse();
        summary.Message.Title.Should().Be("Outbox delivery is pending.");
    }

    private static DashboardOverview Overview(
        int pendingRuns = 0,
        int readyWorkers = 1,
        int availableSlots = 1,
        int pendingOutboxMessages = 0,
        IReadOnlyList<DashboardQueueSummary>? queues = null) => new(
            DateTimeOffset.UtcNow,
            pendingRuns,
            0,
            0,
            0,
            0,
            0,
            readyWorkers,
            0,
            readyWorkers,
            availableSlots,
            0,
            0,
            pendingOutboxMessages,
            new DashboardActivitySummary(0, 0, 0, 0),
            queues ?? Array.Empty<DashboardQueueSummary>(),
            Array.Empty<DashboardRunSummary>());
}
