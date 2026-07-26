namespace KubeJob.Core.Runtime;

/// <summary>
/// Identifies a permanent protocol or submission validation failure.
/// Transport adapters can map this to a client error or dead-letter decision
/// without treating storage and infrastructure failures as invalid input.
/// </summary>
public sealed class ControlPlaneValidationException : ArgumentException
{
    public ControlPlaneValidationException(string code, string message)
        : base(message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        Code = code;
    }

    public string Code { get; }
}
