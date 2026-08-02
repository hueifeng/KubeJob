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
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false
        });

    /// <summary>
    /// Reader the publisher awaits in parallel with <c>Task.Delay(OutboxPollInterval)</c>.
    /// </summary>
    public ChannelReader<bool> Reader => _channel.Reader;

    /// <summary>
    /// Non-blocking wake hint. Safe to call from any thread; never throws, never blocks.
    /// </summary>
    public void Signal() => _channel.Writer.TryWrite(true);

    public void Dispose() => _channel.Writer.TryComplete();
}
