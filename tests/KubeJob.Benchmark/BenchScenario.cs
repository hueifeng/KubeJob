using KubeJob.Core.Runtime;

namespace KubeJob.Benchmark;

/// <summary>
/// Comparison scenarios. Ordering mode is a deployment-level queue policy
/// (see <see cref="QueueDeliveryOptions"/>), so each scenario maps to a
/// distinct logical queue whose ordering is configured at host build time.
/// The per-submission <c>ConcurrencyKey</c> is the application-side lever that
/// selects which KeyOrdered partition a Run belongs to.
/// </summary>
public enum BenchScenario
{
    /// <summary>
    /// No <c>ConcurrencyKey</c>; the queue runs <see cref="ExecutionOrderingMode.Parallel"/>.
    /// Every accepted Run can execute concurrently up to worker capacity.
    /// </summary>
    Parallel,

    /// <summary>
    /// <see cref="ExecutionOrderingMode.KeyOrdered"/> with a large key space
    /// (one distinct key per Run by default). Ordering bookkeeping is present
    /// but contention is low, isolating the per-key gate overhead.
    /// </summary>
    KeyOrderedUniform,

    /// <summary>
    /// <see cref="ExecutionOrderingMode.KeyOrdered"/> with a small key space
    /// (a handful of keys). High per-key contention serializes execution and
    /// exposes how the ordering gate scales under a hot key.
    /// </summary>
    KeyOrderedHotKey,

    /// <summary>
    /// <see cref="ExecutionOrderingMode.StrictFifo"/>: the entire queue/lane
    /// is a single logical worker. Prefetch=1, SAC. Models use cases that
    /// require total global ordering (e.g. ledger, sequential pipeline).
    /// </summary>
    StrictFifo
}

public static class BenchScenarioExtensions
{
    public static ExecutionOrderingMode OrderingMode(this BenchScenario scenario) =>
        scenario switch
        {
            BenchScenario.Parallel => ExecutionOrderingMode.Parallel,
            BenchScenario.KeyOrderedUniform or BenchScenario.KeyOrderedHotKey
                => ExecutionOrderingMode.KeyOrdered,
            BenchScenario.StrictFifo => ExecutionOrderingMode.StrictFifo,
            _ => throw new ArgumentOutOfRangeException(nameof(scenario))
        };

    /// <summary>
    /// Stable logical queue name per scenario. A fresh database is created per
    /// scenario run, so a fixed queue name does not collide across runs; the
    /// RabbitMQ consumer group is still made unique per run for topology
    /// isolation and cleanup.
    /// </summary>
    public static string QueueName(this BenchScenario scenario) => scenario switch
    {
        BenchScenario.Parallel => "bench.parallel",
        BenchScenario.KeyOrderedUniform => "bench.ordered-uniform",
        BenchScenario.KeyOrderedHotKey => "bench.ordered-hotkey",
        BenchScenario.StrictFifo => "bench.strictfifo",
        _ => throw new ArgumentOutOfRangeException(nameof(scenario))
    };

    /// <summary>
    /// Resolves the <c>ConcurrencyKey</c> for one submitted Run under a scenario.
    /// Returns <c>null</c> for the Parallel scenario (no key). Uniform uses a
    /// distinct key per Run; HotKey cycles through a small key space.
    /// </summary>
    public static string? ConcurrencyKey(
        this BenchScenario scenario,
        int runIndex,
        int hotKeyCardinality,
        int uniformKeyCardinality)
    {
        return scenario switch
        {
            BenchScenario.Parallel => null,
            BenchScenario.StrictFifo => null,
            BenchScenario.KeyOrderedUniform => uniformKeyCardinality <= 0
                ? $"k{runIndex}"
                : $"k{runIndex % uniformKeyCardinality}",
            BenchScenario.KeyOrderedHotKey => $"k{runIndex % Math.Max(1, hotKeyCardinality)}",
            _ => throw new ArgumentOutOfRangeException(nameof(scenario))
    };
    }

    public static string Label(this BenchScenario scenario) => scenario switch
    {
        BenchScenario.Parallel => "Parallel",
        BenchScenario.KeyOrderedUniform => "KeyOrdered-Uniform",
        BenchScenario.KeyOrderedHotKey => "KeyOrdered-HotKey",
        BenchScenario.StrictFifo => "StrictFifo",
        _ => scenario.ToString()
    };
}
