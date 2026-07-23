using KubeJob.Core.Context;

namespace KubeJob.Core.Interfaces;

/// <summary>Low-allocation handler that consumes raw UTF-8 JSON from the execution context.</summary>
public interface IKubeJobV2
{
    ValueTask ExecuteAsync(KubeJobContextV2 context, CancellationToken cancellationToken);
}

/// <summary>Strongly typed handler. Payload deserialization happens once per execution.</summary>
public interface IKubeJob<TPayload>
{
    ValueTask ExecuteAsync(
        TPayload payload,
        KubeJobContextV2 context,
        CancellationToken cancellationToken);
}
