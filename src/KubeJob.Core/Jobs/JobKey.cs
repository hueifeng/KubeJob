namespace KubeJob.Core.Jobs;

/// <summary>
/// Identifies a registered job and binds the identifier to its payload contract.
/// </summary>
/// <typeparam name="TPayload">The payload type accepted by the job.</typeparam>
public readonly record struct JobKey<TPayload>
{
    /// <summary>
    /// Initializes a new job key.
    /// </summary>
    /// <param name="value">A stable, version-independent job identifier.</param>
    public JobKey(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        Value = value;
    }

    /// <summary>
    /// Gets the stable job identifier.
    /// </summary>
    public string Value { get; } = string.Empty;

    /// <summary>
    /// Gets whether this instance is the default, uninitialized key.
    /// </summary>
    public bool IsEmpty => string.IsNullOrEmpty(Value);

    /// <inheritdoc />
    public override string ToString() => Value;
}
