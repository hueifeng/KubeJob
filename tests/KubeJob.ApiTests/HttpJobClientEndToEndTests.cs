using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using KubeJob.Client;
using KubeJob.Core.Client;
using KubeJob.Core.Jobs;
using KubeJob.Core.Runtime;
using KubeJob.Core.Transport;
using KubeJob.Server.Extensions;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Server.Runtime;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KubeJob.ApiTests;

public sealed record RemotePayload(string Value);

public sealed class HttpJobClientEndToEndTests
{
    [Fact]
    public async Task Client_can_enqueue_query_and_cancel_over_http()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);

        var handle = await client.EnqueueAsync(
            new JobKey<RemotePayload>("remote.echo"),
            new RemotePayload("hello"),
            new JobEnqueueOptions
            {
                Queue = "default",
                IdempotencyKey = "remote:1"
            });
        var pending = await client.GetStatusAsync(handle.JobId);
        var canceled = await client.CancelAsync(handle.JobId, "test cancellation");
        var terminal = await client.GetStatusAsync(handle.JobId);

        pending.Should().NotBeNull();
        pending!.Phase.Should().Be(JobPhase.Pending);
        canceled.Should().BeTrue();
        terminal!.Phase.Should().Be(JobPhase.Canceled);
    }

    [Fact]
    public async Task Client_uses_the_job_key_as_the_default_logical_queue()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);

        await client.EnqueueAsync(
            new JobKey<RemotePayload>("remote.echo"),
            new RemotePayload("hello"));

        var runs = await app.Services.GetRequiredService<IJobRuntimeDashboardStore>()
            .GetRunsAsync(
                new DashboardRunQuery(PageSize: 10, JobKey: "remote.echo", ExactJobKey: true),
                CancellationToken.None);

        runs.Items.Should().ContainSingle()
            .Which.Queue.Should().Be("remote.echo");
    }

    [Fact]
    public async Task Client_surfaces_idempotency_conflict_from_server()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);
        var key = new JobKey<RemotePayload>("remote.echo");
        var options = new JobEnqueueOptions
        {
            IdempotencyKey = "remote:conflict"
        };

        var first = await client.EnqueueAsync(
            key,
            new RemotePayload("first"),
            options);
        var action = async () => await client.EnqueueAsync(
            key,
            new RemotePayload("different"),
            options);

        var exception = await action.Should().ThrowAsync<IdempotencyConflictException>();
        exception.Which.IdempotencyKey.Should().Be("remote:conflict");
        exception.Which.ExistingJobId.Should().Be(first.JobId);
    }

    [Fact]
    public async Task Client_uses_the_atomic_server_batch_endpoint()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);
        var key = new JobKey<RemotePayload>("remote.batch");
        var batch = new (RemotePayload Payload, JobEnqueueOptions? Options)[]
        {
            (new RemotePayload("first"), new JobEnqueueOptions
            {
                IdempotencyKey = "remote:batch:1"
            }),
            (new RemotePayload("second"), new JobEnqueueOptions
            {
                IdempotencyKey = "remote:batch:2"
            })
        };

        var handles = await client.EnqueueBatchAsync(key, batch);
        var replay = await client.EnqueueBatchAsync(key, batch);
        var runs = await app.Services.GetRequiredService<IJobRuntimeDashboardStore>()
            .GetRunsAsync(
                new DashboardRunQuery(PageSize: 10, JobKey: "remote.batch", ExactJobKey: true),
                CancellationToken.None);

        handles.Should().HaveCount(2);
        replay.Should().Equal(handles);
        runs.TotalCount.Should().Be(2);
    }

    [Fact]
    public async Task Batch_endpoint_rejects_null_items_as_a_validation_error()
    {
        await using var app = await StartServerAsync();
        using var http = app.GetTestClient();

        using var response = await http.PostAsJsonAsync<object?[]>(
            "api/kubejob/jobs/batch",
            new object?[] { null });
        var body = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        body.Should().Contain("invalid_job_submission");
        var runs = await app.Services.GetRequiredService<IJobRuntimeDashboardStore>()
            .GetRunsAsync(new DashboardRunQuery(PageSize: 10), CancellationToken.None);
        runs.TotalCount.Should().Be(0);
    }

    [Fact]
    public async Task Client_routes_a_broker_native_queue_to_the_transport_without_creating_a_managed_run()
    {
        var (app, publisher) = await StartBrokerNativeServerAsync();
        await using var ownedApp = app;
        using var http = app.GetTestClient();
        var client = new HttpJobClient(http);

        var handle = await client.EnqueueAsync(
            new JobKey<RemotePayload>("remote.native"),
            new RemotePayload("hello"),
            new JobEnqueueOptions { Queue = "remote.native" });

        publisher.Requests.Should().ContainSingle();
        var message = JsonSerializer.Deserialize<BrokerNativeJobMessage>(
            publisher.Requests[0].Message.Body.Span,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        message!.MessageId.Should().Be(handle.JobId);
        message.Queue.Should().Be("remote.native");

        var run = await app.Services.GetRequiredService<IJobQueryStore>()
            .GetRunAsync(handle.JobId, CancellationToken.None);
        run.Should().BeNull();
    }

    [Fact]
    public async Task Broker_native_endpoint_rejects_managed_idempotency_options_as_a_client_error()
    {
        var (app, publisher) = await StartBrokerNativeServerAsync();
        await using var ownedApp = app;
        using var http = app.GetTestClient();

        using var response = await http.PostAsJsonAsync(
            "api/kubejob/jobs",
            new EnqueueJobRequest(
                "remote.native",
                JsonSerializer.Serialize(new RemotePayload("hello")),
                Queue: "remote.native",
                IdempotencyKey: "managed-only"));
        var body = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        body.Should().Contain("unsupported_job_submission");
        publisher.Requests.Should().BeEmpty();
    }

    private static async Task<WebApplication> StartServerAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddKubeJobServer(options => options.AllowAnonymousEndpoints = true);

        var app = builder.Build();
        app.MapControllers();
        await app.StartAsync();
        return app;
    }

    private static async Task<(WebApplication App, RecordingTransportPublisher Publisher)>
        StartBrokerNativeServerAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddKubeJobServer(options => options.AllowAnonymousEndpoints = true);
        builder.Services.ConfigureKubeJobQueueRuntimes(options =>
        {
            options.Queues["remote.native"] = new QueueRuntimeRoute
            {
                Mode = QueueRuntimeMode.BrokerNative,
                TransportId = "recording"
            };
        });
        var publisher = new RecordingTransportPublisher();
        builder.Services.AddSingleton<IMessageTransportPublisher>(publisher);

        var app = builder.Build();
        app.MapControllers();
        await app.StartAsync();
        return (app, publisher);
    }

    private sealed class RecordingTransportPublisher : IMessageTransportPublisher
    {
        public string TransportId => "recording";

        public MessageTransportCapabilities Capabilities =>
            MessageTransportCapabilities.DurablePublish
            | MessageTransportCapabilities.DeadLetter;

        public List<TransportPublishRequest> Requests { get; } = new();

        public ValueTask PublishAsync(
            TransportPublishRequest request,
            CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            return ValueTask.CompletedTask;
        }
    }
}
