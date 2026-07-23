using System.Collections.Concurrent;
using FluentAssertions;
using KubeJob.Core.Attributes;
using KubeJob.Core.Client;
using KubeJob.Core.Execution;
using KubeJob.Core.Interfaces;
using KubeJob.Server.Extensions;
using KubeJob.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace KubeJob.EndToEndTests;

public sealed record EchoPayload(string Value);

[KubeJob("e2e.echo")]
public sealed class EchoJob : IKubeJob<EchoPayload>
{
    private readonly ExecutionProbe _probe;

    public EchoJob(ExecutionProbe probe)
    {
        _probe = probe;
    }

    public ValueTask ExecuteAsync(
        EchoPayload payload,
        JobExecutionContext context,
        CancellationToken cancellationToken)
    {
        _probe.Executions.Enqueue(new ExecutionObservation(
            payload.Value,
            context.RunId,
            context.AttemptId,
            context.AttemptNumber,
            context.Worker.WorkerId,
            context.Worker.SessionId));
        return ValueTask.CompletedTask;
    }
}

public sealed record ExecutionObservation(
    string Value,
    string RunId,
    string AttemptId,
    int AttemptNumber,
    string WorkerId,
    string SessionId);

public sealed class ExecutionProbe
{
    public ConcurrentQueue<ExecutionObservation> Executions { get; } = new();
}

public sealed class UnifiedRuntimeEndToEndTests
{
    [Fact]
    public async Task Typed_job_completes_through_in_process_worker_transport()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddLogging();
        builder.Services.AddKubeJobServer();
        builder.Services.UseInProcessKubeJobWorkerTransport();
        builder.Services.AddKubeJobWorkerRuntime(options =>
        {
            options.ServerEndpoint = "http://unused.local/";
            options.WorkerId = "unified-e2e-worker";
            options.BuildId = "e2e";
            options.MaxConcurrentJobs = 2;
            options.ClaimBatchSize = 2;
            options.EmptyPollDelay = TimeSpan.FromMilliseconds(50);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.LeaseRenewalInterval = TimeSpan.FromMilliseconds(100);
            options.DrainTimeout = TimeSpan.FromSeconds(2);
        });
        builder.Services.AddKubeJobHandler<EchoJob, EchoPayload>();
        builder.Services.AddSingleton<ExecutionProbe>();

        await using var host = builder.Build();
        await host.StartAsync();

        try
        {
            var client = host.Services.GetRequiredService<IJobClient>();
            var handle = await client.EnqueueAsync(
                Jobs.Echo,
                new EchoPayload("hello"),
                new JobEnqueueOptions
                {
                    Queue = "default",
                    MaxAttempts = 2,
                    Timeout = TimeSpan.FromSeconds(10)
                });

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var status = await client.WaitForCompletionAsync(
                handle,
                TimeSpan.FromMilliseconds(50),
                timeout.Token);
            var observation = host.Services
                .GetRequiredService<ExecutionProbe>()
                .Executions.Single();

            status.Phase.Should().Be(JobPhase.Succeeded);
            status.Attempt.Should().Be(1);
            observation.Value.Should().Be("hello");
            observation.RunId.Should().Be(handle.JobId);
            observation.AttemptNumber.Should().Be(1);
            observation.WorkerId.Should().Be("unified-e2e-worker");
            observation.SessionId.Should().NotBeNullOrWhiteSpace();
        }
        finally
        {
            using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await host.StopAsync(stopTimeout.Token);
        }
    }
}
