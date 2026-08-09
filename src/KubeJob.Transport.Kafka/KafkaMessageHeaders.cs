using System.Globalization;
using System.Text;
using Confluent.Kafka;
using KubeJob.Core.Transport;

namespace KubeJob.Transport.Kafka;

internal static class KafkaMessageHeaders
{
    public const string MessageId = "x-kubejob-message-id";
    public const string MessageType = "x-kubejob-message-type";
    public const string CorrelationId = "x-kubejob-correlation-id";
    public const string NotBeforeUnixMilliseconds = "x-kubejob-not-before-unix-ms";

    public static Headers Create(TransportMessage message)
    {
        var headers = new Headers();
        Add(headers, MessageId, message.MessageId);
        Add(headers, MessageType, message.MessageType);
        Add(headers, CorrelationId, message.CorrelationId);

        if (message.Headers is not null)
        {
            foreach (var pair in message.Headers)
            {
                Add(headers, pair.Key, pair.Value);
            }
        }

        return headers;
    }

    public static Headers CopyWithNotBefore(Headers? source, DateTimeOffset notBefore)
    {
        var headers = new Headers();
        if (source is not null)
        {
            foreach (var header in source)
            {
                headers.Add(header.Key, header.GetValueBytes());
            }
        }

        Add(headers, NotBeforeUnixMilliseconds, notBefore.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        return headers;
    }

    public static DateTimeOffset? GetNotBefore(Headers? headers)
    {
        var value = GetString(headers, NotBeforeUnixMilliseconds);
        return long.TryParse(value, CultureInfo.InvariantCulture, out var milliseconds)
            ? DateTimeOffset.FromUnixTimeMilliseconds(milliseconds)
            : null;
    }

    public static string? GetString(Headers? headers, string name)
    {
        var raw = headers is null || !headers.Any(header => string.Equals(header.Key, name, StringComparison.Ordinal))
            ? null
            : headers.GetLastBytes(name);
        return raw is null ? null : Encoding.UTF8.GetString(raw);
    }

    private static void Add(Headers headers, string key, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }
    }
}
