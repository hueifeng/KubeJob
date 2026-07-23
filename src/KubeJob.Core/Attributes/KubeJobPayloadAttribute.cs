namespace KubeJob.Core.Attributes;

/// <summary>Declares the payload schema understood by a typed job handler.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class KubeJobPayloadAttribute : Attribute
{
    public KubeJobPayloadAttribute(int schemaVersion = 1)
    {
        if (schemaVersion <= 0) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        SchemaVersion = schemaVersion;
    }

    public int SchemaVersion { get; }
    public string HandlerVersion { get; set; } = string.Empty;
}
