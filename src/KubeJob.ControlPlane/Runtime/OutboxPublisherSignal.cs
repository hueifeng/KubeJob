using System.Threading.Channels;

namespace KubeJob.ControlPlane.Runtime;

/// <summary>
/// In-process wake-up signal that lets a same-process writer notify the
/// <see cref="OutboxPublisherService"/> the moment a new outbox row commits,
/// without waiting for the next <c>OutboxPollInterval</c> tick. The polling
/// loop remains as the cross-process / cold-start fallback; this signal just
/// collapses the idle wait when the writer and publisher share a process.
/// </summary>
/// <remarks>
/// Multiple <see cref="Signal"/> calls between two wakes are coalesced into a
/// single wake: the underlying channel has capacity one with
/// <see cref="BoundedChannelFullMode.DropWrite"/>, so a full signal buffer
/// means "already pending" and any further writes are dropped. The publisher
/// reader is the sole consumer (<see cref="BoundedChannelOptions.SingleReader"/>).
/// </remarks>
public sealed class OutboxPublisherSignal : IDisposable
{
    private readonly bool _enabled;
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false
        });

    /// <summary>
    /// Direct construction remains enabled for focused runtime tests and custom
    /// hosts. Server composition disables this signal when the effective
    /// WorkAvailable notifier is the no-op implementation, because no new wake
    /// outbox rows are generated in that configuration.
    /// </summary>
    public OutboxPublisherSignal(bool enabled = true)
    {
        _enabled = enabled;
    }

    /// <summary>
    /// Reader the publisher awaits in parallel with <c>Task.Delay(OutboxPollInterval)</c>.
    /// </summary>
    public ChannelReader<bool> Reader => _channel.Reader;

    /// <summary>
    /// Non-blocking wake hint. Safe to call from any thread; never throws, never blocks.
    /// When disabled, calls are intentionally ignored and the outbox publisher
    /// relies on its low-frequency poll only to drain legacy pending rows.
    /// </summary>
    public void Signal()
    {
        if (_enabled)
        {
            _channel.Writer.TryWrite(true);
        }
    }

    public void Dispose() => _channel.Writer.TryComplete();
}
