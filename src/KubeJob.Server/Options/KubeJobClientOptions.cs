using System.Text.Json;

namespace KubeJob.Server.Options;

public sealed class KubeJobClientOptions
{
    public int MaxPayloadBytes { get; set; } = 1024 * 1024;
    public JsonSerializerOptions PayloadJsonOptions { get; set; } = new(JsonSerializerDefaults.Web);
}
