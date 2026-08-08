using System.Text.Json;
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
            ExecutionLane = "legacy-lane",
            ConsumerGroup = "legacy-group",
            PartitionKey = "legacy-key",
            EventType = OutboxEventTypes.WorkAvailable,
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
    public void Work_available_wire_contract_does_not_leak_legacy_routing_dimensions()
    {
        var signal = WorkAvailableSignal.FromOutbox(new OutboxMessageRecord
        {
            Id = "outbox-1",
            Queue = "mail",
            ExecutionLane = "legacy-lane",
            ConsumerGroup = "legacy-group",
            PartitionKey = "legacy-key",
            EventType = OutboxEventTypes.WorkAvailable,
            PayloadJson = "{\"runId\":\"run-1\",\"queue\":\"mail\"}",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow
        });

        var json = JsonSerializer.Serialize(signal, new JsonSerializerOptions(JsonSerializerDefaults.Web));

        json.Should().Contain("\"schemaVersion\":2");
        json.Should().Contain("\"queue\":\"mail\"");
        json.Should().Contain("\"runId\":\"run-1\"");
        json.Should().NotContain("executionLane");
        json.Should().NotContain("consumerGroup");
        json.Should().NotContain("partitionKey");
    }

    [Fact]
    public void Mismatched_outbox_queue_is_rejected()
    {
        var message = new OutboxMessageRecord
        {
            Id = "outbox-1",
            Queue = "mail",
            EventType = OutboxEventTypes.WorkAvailable,
            PayloadJson = "{\"runId\":\"run-1\",\"queue\":\"reports\"}",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow
        };

        var action = () => WorkAvailableSignal.FromOutbox(message);

        action.Should().Throw<InvalidOperationException>();
    }
}
