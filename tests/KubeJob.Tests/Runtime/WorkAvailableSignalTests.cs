using FluentAssertions;
using KubeJob.Core.Runtime;

namespace KubeJob.Tests.Runtime;

public sealed class WorkAvailableSignalTests
{
    [Fact]
    public void Valid_work_available_outbox_message_becomes_a_versioned_signal()
    {
        var signal = WorkAvailableSignal.FromOutbox(new OutboxMessageRecord
        {
            Id = "outbox-1",
            Queue = "mail",
            EventType = "work-available",
            PayloadJson = "{\"runId\":\"run-1\",\"queue\":\"mail\"}",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow
        });

        signal.SchemaVersion.Should().Be(WorkAvailableSignal.CurrentSchemaVersion);
        signal.EventId.Should().Be("outbox-1");
        signal.Queue.Should().Be("mail");
        signal.RunId.Should().Be("run-1");
    }

    [Fact]
    public void Mismatched_outbox_queue_is_rejected()
    {
        var message = new OutboxMessageRecord
        {
            Id = "outbox-1",
            Queue = "mail",
            EventType = "work-available",
            PayloadJson = "{\"runId\":\"run-1\",\"queue\":\"reports\"}",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow
        };

        var action = () => WorkAvailableSignal.FromOutbox(message);

        action.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Malformed_work_available_payload_is_rejected()
    {
        var message = new OutboxMessageRecord
        {
            Id = "outbox-malformed",
            Queue = "mail",
            EventType = "work-available",
            PayloadJson = "{not-json}",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow
        };

        var action = () => WorkAvailableSignal.FromOutbox(message);

        action.Should().Throw<System.Text.Json.JsonException>();
    }
}
