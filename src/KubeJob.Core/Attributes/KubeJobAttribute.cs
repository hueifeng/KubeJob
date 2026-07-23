namespace KubeJob.Core.Attributes;

/// <summary>
/// Declares the stable public key of a typed KubeJob handler.
/// Scheduling, retries, timeout, queues, placement, and batching belong to
/// submissions or Schedule resources rather than handler metadata.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class KubeJobAttribute : Attribute
{
    public KubeJobAttribute(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("A stable job key is required.", nameof(key));
        }

        Key = key.Trim();
    }

    public string Key { get; }
}
