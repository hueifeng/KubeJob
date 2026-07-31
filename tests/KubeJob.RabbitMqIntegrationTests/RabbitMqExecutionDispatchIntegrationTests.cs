using FluentAssertions;
using KubeJob;
using KubeJob.Core.Attributes;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Server.Extensions;
using KubeJob.Transport.RabbitMQ;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace KubeJob.RabbitMqIntegrationTests;

[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class RabbitMqExecutionDispatchIntegrationTests
{
    private static readonly JobKey<ExecutionDispatchPayload> JobKey = new("execution-dispatch-test");

    [Fact]
    public async Task Happy_path_submits_dispatches_and_completes_via_broker_admission()
    {
        var connectionString = RequireConnectionString();
        var group = $"exec-happy-{Guid.NewGuid():N}";

        using var host = BuildHost(connectionString, group, out _);
        await host.StartAsync();
        try
        {
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var handle = await jobs.EnqueueAsync(
                JobKey,
                new ExecutionDispatchPayload("succeed", FailAttempts: 0),
                new JobEnqueueOptions { Queue = "default", MaxAttempts = 3 });

            var status = await jobs.WaitForCompletionAsync(
                handle,
                pollInterval: TimeSpan.FromMilliseconds(100),
                cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

            status.Phase.Should().Be(JobPhase.Succeeded);
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connectionString, group);
        }
    }

    [Fact]
    public async Task Retry_path_reconciles_through_the_broker_retry_queue_then_succeeds()
    {
        var connectionString = RequireConnectionString();
        var group = $"exec-retry-{Guid.NewGuid():N}";

        using var host = BuildHost(connectionString, group, out _);
        await host.StartAsync();
        try
        {
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var handle = await jobs.EnqueueAsync(
                JobKey,
                new ExecutionDispatchPayload("fail-then-succeed", FailAttempts: 2),
                new JobEnqueueOptions { Queue = "default", MaxAttempts = 5 });

            var status = await jobs.WaitForCompletionAsync(
                handle,
                pollInterval: TimeSpan.FromMilliseconds(100),
                cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

            status.Phase.Should().Be(JobPhase.Succeeded);
            status.AttemptCount.Should().BeGreaterThan(1);
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connectionString, group);
        }
    }

    [Fact]
    public async Task Reconciliation_path_recovers_execution_after_broker_retry_budget_is_exhausted()
    {
        var connectionString = RequireConnectionString();
        var group = $"exec-reconcile-{Guid.NewGuid():N}";

        using var host = BuildHost(
            connectionString,
            group,
            out _,
            maxBrokerRetryAttempts: 1,
            brokerRetryReconciliationDelay: TimeSpan.FromSeconds(2));
        await host.StartAsync();
        try
        {
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var startedAt = DateTimeOffset.UtcNow;
            var handle = await jobs.EnqueueAsync(
                JobKey,
                new ExecutionDispatchPayload("fail-then-succeed", FailAttempts: 2),
                new JobEnqueueOptions { Queue = "default", MaxAttempts = 5 });

            var status = await jobs.WaitForCompletionAsync(
                handle,
                pollInterval: TimeSpan.FromMilliseconds(100),
                cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

            status.Phase.Should().Be(JobPhase.Succeeded);
            status.AttemptCount.Should().BeGreaterThan(1);
            (DateTimeOffset.UtcNow - startedAt).Should().BeGreaterThanOrEqualTo(
                TimeSpan.FromSeconds(2),
                "exhausting MaxBrokerRetryAttempts=1 on the second failure must hand off to durable "
                    + "Postgres/in-memory reconciliation (RequeueExecutionAsync) gated by "
                    + "BrokerRetryReconciliationDelay, not an immediate broker-level retry");
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connectionString, group);
        }
    }

    [Fact]
    public async Task Cancel_signal_aborts_the_in_flight_attempt_through_the_broker_cancel_queue()
    {
        var connectionString = RequireConnectionString();
        var group = $"exec-cancel-{Guid.NewGuid():N}";

        using var host = BuildHost(connectionString, group, out _, enableCancelQueue: true);
        await host.StartAsync();
        try
        {
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var handle = await jobs.EnqueueAsync(
                JobKey,
                new ExecutionDispatchPayload("wait-for-cancel", FailAttempts: 0),
                new JobEnqueueOptions { Queue = "default", MaxAttempts = 1 });

            await EventuallyAsync(async () =>
            {
                var running = await jobs.GetStatusAsync(handle.JobId);
                return running?.Phase == JobPhase.Running;
            });

            var canceled = await jobs.CancelAsync(handle.JobId, "test cancellation");
            canceled.Should().BeTrue();

            var status = await jobs.WaitForCompletionAsync(
                handle,
                pollInterval: TimeSpan.FromMilliseconds(100),
                cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

            status.Phase.Should().Be(JobPhase.Canceled);
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connectionString, group);
        }
    }

    [Fact]
    public async Task Reject_path_drops_malformed_envelope_to_the_group_dlq_without_wedging_the_consumer()
    {
        var connectionString = RequireConnectionString();
        var group = $"exec-reject-{Guid.NewGuid():N}";

        using var host = BuildHost(connectionString, group, out var options);
        await host.StartAsync();
        try
        {
            using var connection = CreateConnection(connectionString);
            using var channel = connection.CreateModel();
            await EventuallyAsync(
                () => Task.FromResult(channel.ConsumerCount(options.GetSharedConsumerQueueName()) >= 1),
                attempts: 200);

            var properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.MessageId = $"malformed-{Guid.NewGuid():N}";
            channel.BasicPublish(
                options.GetGroupExchangeName(),
                "default",
                mandatory: false,
                basicProperties: properties,
                body: System.Text.Encoding.UTF8.GetBytes("not-json"));

            await EventuallyAsync(
                () => Task.FromResult(channel.MessageCount(options.GetGroupDlqName()) == 1),
                attempts: 200);

            // The consumer must still be alive after the reject: prove it by
            // successfully completing a normal Run through the same channel.
            var jobs = host.Services.GetRequiredService<IJobClient>();
            var handle = await jobs.EnqueueAsync(
                JobKey,
                new ExecutionDispatchPayload("succeed", FailAttempts: 0),
                new JobEnqueueOptions { Queue = "default", MaxAttempts = 1 });
            var status = await jobs.WaitForCompletionAsync(
                handle,
                pollInterval: TimeSpan.FromMilliseconds(100),
                cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);
            status.Phase.Should().Be(JobPhase.Succeeded);
        }
        finally
        {
            await host.StopAsync();
            DeleteGroupTopology(connectionString, group);
        }
    }

    private static IHost BuildHost(
        string connectionString,
        string group,
        out RabbitMqExecutionOptions capturedOptions,
        int maxBrokerRetryAttempts = 2,
        TimeSpan? brokerRetryReconciliationDelay = null,
        bool enableCancelQueue = false)
    {
        var options = new RabbitMqExecutionOptions
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.execution",
            MaxBrokerRetryAttempts = maxBrokerRetryAttempts,
            RetryDelay = TimeSpan.FromMilliseconds(200),
            BrokerRetryReconciliationDelay = brokerRetryReconciliationDelay ?? TimeSpan.FromSeconds(2),
            EnableCancelQueue = enableCancelQueue
        };
        capturedOptions = options;

        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddKubeJobHandler<ExecutionDispatchJob, ExecutionDispatchPayload>(JobKey);
                services.AddKubeJob(
                    configureServer: server => server.UseInMemory(),
                    configureWorker: worker =>
                    {
                        worker.WorkerId = $"worker-{group}";
                        worker.Queues = new List<string> { "default" };
                        worker.MaxConcurrentJobs = 4;
                        worker.EmptyPollDelay = TimeSpan.FromSeconds(30);
                    });
                services.ConfigureKubeJobQueueRouting(routing =>
                {
                    routing.DefaultProfile = ExecutionDeliveryProfile.BrokerDispatch;
                    routing.DefaultTransportId = "rabbitmq";
                });
                services.UseRabbitMqKubeJobExecutionDispatcher(o => CopyInto(options, o));
                services.AddRabbitMqKubeJobExecutionConsumer(o => CopyInto(options, o));
                if (options.EnableCancelQueue)
                {
                    services.UseRabbitMqKubeJobCancelPublisher(o => CopyInto(options, o));
                }
            })
            .Build();
        return host;
    }

    private static void CopyInto(RabbitMqExecutionOptions source, RabbitMqExecutionOptions target)
    {
        target.ConnectionString = source.ConnectionString;
        target.ConsumerGroup = source.ConsumerGroup;
        target.ConsumerQueuePrefix = source.ConsumerQueuePrefix;
        target.MaxBrokerRetryAttempts = source.MaxBrokerRetryAttempts;
        target.RetryDelay = source.RetryDelay;
        target.BrokerRetryReconciliationDelay = source.BrokerRetryReconciliationDelay;
        target.EnableCancelQueue = source.EnableCancelQueue;
    }

    private static string RequireConnectionString() =>
        Environment.GetEnvironmentVariable("KUBEJOB_RABBITMQ_TEST_CONNECTION")
            ?? throw new InvalidOperationException(
                "Set KUBEJOB_RABBITMQ_TEST_CONNECTION before running this integration project.");

    private static IConnection CreateConnection(string connectionString) =>
        new ConnectionFactory
        {
            Uri = new Uri(connectionString, UriKind.Absolute)
        }.CreateConnection("KubeJob.Tests.RabbitMqExecutionDispatch");

    private static void DeleteGroupTopology(string connectionString, string group)
    {
        var options = new RabbitMqExecutionOptions
        {
            ConnectionString = connectionString,
            ConsumerGroup = group,
            ConsumerQueuePrefix = "kubejob.test.execution"
        };
        using var connection = CreateConnection(connectionString);
        using var channel = connection.CreateModel();
        try
        {
            channel.QueueDelete(options.GetSharedConsumerQueueName(), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(options.GetSharedRetryQueueName(), ifUnused: false, ifEmpty: false);
            channel.QueueDelete(options.GetGroupDlqName(), ifUnused: false, ifEmpty: false);
            channel.ExchangeDelete(options.GetGroupExchangeName());
            channel.ExchangeDelete(options.GetRetryExchangeName());
            channel.ExchangeDelete(options.GetGroupDlxName());
        }
        catch (Exception)
        {
            // Best-effort cleanup; leaving stray broker topology behind does
            // not fail the test run.
        }
    }

    private static async Task EventuallyAsync(
        Func<Task<bool>> condition,
        int attempts = 50)
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

    private sealed record ExecutionDispatchPayload(string Scenario, int FailAttempts);

    [KubeJob("execution-dispatch-test")]
    private sealed class ExecutionDispatchJob : IKubeJob<ExecutionDispatchPayload>
    {
        public ValueTask ExecuteAsync(
            ExecutionDispatchPayload payload,
            JobExecutionContext context,
            CancellationToken cancellationToken)
        {
            if (payload.Scenario == "fail-then-succeed" && context.AttemptNumber <= payload.FailAttempts)
            {
                throw new InvalidOperationException(
                    $"Simulated transient failure on attempt {context.AttemptNumber}.");
            }

            if (payload.Scenario == "wait-for-cancel")
            {
                return WaitForCancellationAsync(cancellationToken);
            }

            return ValueTask.CompletedTask;
        }

        private static async ValueTask WaitForCancellationAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
    }
}
