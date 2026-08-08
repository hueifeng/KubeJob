using System.Text;
using System.Text.Json;
using FluentAssertions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Server.Runtime;
using KubeJob.Transport.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace KubeJob.RabbitMqIntegrationTests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class RabbitMqIntegrationCollection
{
    public const string Name = "rabbitmq-integration";
}

[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqIngressIntegrationTests
{
    [Fact]
    public async Task Ingress_acks_accepted_messages_and_dead_letters_invalid_json()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

        var suffix = Guid.NewGuid().ToString("N");
        var exchange = $"kubejob.test.ingress.{suffix}";
        var queue = $"kubejob.test.ingress.queue.{suffix}";
        var deadLetterExchange = $"kubejob.test.dlx.{suffix}";
        var deadLetterQueue = $"kubejob.test.dlq.{suffix}";

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddKubeJobServer();
                services.AddRabbitMqKubeJobIngress(options =>
                {
                    options.ConnectionString = connectionString;
                    options.ExchangeName = exchange;
                    options.QueueName = queue;
                    options.RoutingKey = "jobs.#";
                    options.Source = "rabbitmq.integration";
                    options.DeadLetterExchangeName = deadLetterExchange;
                    options.DeadLetterRoutingKey = "dead";
                });
            })
            .Build();

        await host.StartAsync();
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare(deadLetterExchange, ExchangeType.Direct, true, false);
            channel.QueueDeclare(deadLetterQueue, true, false, false);
            channel.QueueBind(deadLetterQueue, deadLetterExchange, "dead");
            await EventuallyAsync(
                () => HasConsumerAsync(connectionString, queue),
                attempts: 1200);

            var valid = new RabbitMqJobIngressEnvelope(
                "integration-message-1",
                "sample.data",
                "{\"value\":1}");
            Publish(channel, exchange, "jobs.sample", valid, valid.MessageId);
            Publish(channel, exchange, "jobs.sample", valid, valid.MessageId);

            await EventuallyAsync(async () =>
            {
                var runs = await host.Services
                    .GetRequiredService<IJobRuntimeDashboardStore>()
                    .GetRunsAsync(
                        new DashboardRunQuery(
                            PageSize: 100,
                            JobKey: "sample.data",
                            ExactJobKey: true),
                        CancellationToken.None);
                return runs.TotalCount == 1;
            }, attempts: 200);

            var invalidBody = Encoding.UTF8.GetBytes("not-json");
            var invalidProperties = channel.CreateBasicProperties();
            invalidProperties.MessageId = "integration-invalid-1";
            channel.BasicPublish(
                exchange,
                "jobs.sample",
                mandatory: false,
                basicProperties: invalidProperties,
                body: invalidBody);

            await EventuallyAsync(
                () => HasMessageCountAsync(connectionString, deadLetterQueue, 1),
                attempts: 200);
            (await GetMessageCountAsync(connectionString, queue)).Should().Be(0);
        }
        finally
        {
            await host.StopAsync();
            using var cleanupConnection = CreateConnection(connectionString);
            using var cleanupChannel = cleanupConnection.CreateModel();
            cleanupChannel.QueueDelete(queue, ifUnused: false, ifEmpty: false);
            cleanupChannel.QueueDelete(deadLetterQueue, ifUnused: false, ifEmpty: false);
            cleanupChannel.ExchangeDelete(deadLetterExchange);
            cleanupChannel.ExchangeDelete(exchange);
        }
    }

    private static IConnection CreateConnection(string connectionString) =>
        new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute)
        }.CreateConnection("KubeJob.Tests.RabbitMqIngress");

    private static async Task<bool> HasConsumerAsync(string connectionString, string queue)
    {
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            return channel.ConsumerCount(queue) == 1;
        }
        catch (OperationInterruptedException)
        {
            await Task.CompletedTask;
            return false;
        }
    }

    private static async Task<bool> HasMessageCountAsync(
        string connectionString,
        string queue,
        uint expected) =>
        await GetMessageCountAsync(connectionString, queue) == expected;

    private static async Task<uint> GetMessageCountAsync(string connectionString, string queue)
    {
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            return channel.MessageCount(queue);
        }
        catch (OperationInterruptedException)
        {
            await Task.CompletedTask;
            return 0;
        }
    }

    private static void Publish(
        IModel channel,
        string exchange,
        string routingKey,
        RabbitMqJobIngressEnvelope envelope,
        string messageId)
    {
        var properties = channel.CreateBasicProperties();
        properties.ContentType = "application/json";
        properties.MessageId = messageId;
        channel.BasicPublish(
            exchange,
            routingKey,
            mandatory: false,
            basicProperties: properties,
            body: Encoding.UTF8.GetBytes(JsonSerializer.Serialize(envelope)));
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, int attempts = 50)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        (await condition()).Should().BeTrue();
    }
}
