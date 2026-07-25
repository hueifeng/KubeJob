namespace KubeJob.Core.Client;

/// <summary>
/// Identifies a submitted logical job run.
/// </summary>
public sealed record JobHandle(
    string JobId,
    string? BatchId = null);
