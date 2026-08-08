using System.Text.Json;
using KubeJob.Core.Events;
using KubeJob.Core.Execution;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Worker.Runtime;

/// <summary>
/// Adapts a typed event subscription to the shared WorkerExecutionEngine.
/// The synthetic HandlerKey is an internal execution identity; applications
/// continue to work with Topic / EventType / Subscription.
/// </summary>
public sealed class EventHandlerInvoker<THandler, TEvent> : IJobHandlerInvoker
    where THandler : class, IKubeEventHandler<TEvent>
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly EventSubscriptionDefinition _subscription;

    public EventHandlerInvoker(EventSubscriptionDefinition subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        _subscription = subscription;
    }

    public string JobKey => _subscription.HandlerKey;

    public Type PayloadType => typeof(TEvent);

    public ValueTask InvokeAsync(
        IServiceProvider serviceProvider,
        string payloadJson,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Deserialize<TEvent>(payloadJson, SerializerOptions);
        if (payload is null)
        {
            throw new JsonException(
                $"Payload for event subscription '{_subscription.Subscription}' deserialized to null.");
        }

        var handler = serviceProvider.GetRequiredService<THandler>();
        var eventContext = new EventExecutionContext
        {
            EventId = context.RunId,
            Topic = _subscription.Topic,
            RoutingKey = _subscription.RoutingKey,
            Subscription = _subscription.Subscription,
            AttemptNumber = context.AttemptNumber,
            Worker = context.Worker,
            ServiceProvider = serviceProvider
        };

        return handler.HandleAsync(payload, eventContext, cancellationToken);
    }
}
