using KubeJob;
using KubeJob.Sample.RemoteWorker.Jobs;
using KubeJob.Server.Extensions;
using KubeJob.Worker.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKubeJobHandler<SampleDataJob, SampleDataPayload>();
builder.Services.AddKubeJob(
    configureServer: options => options.UseInMemory(),
    configureWorker: options =>
    {
        options.WorkerId = "unified-sample";
        options.MaxConcurrentJobs = 10;
        options.Queues = new List<string> { "default", "samples" };
        options.BuildId = typeof(Program).Assembly.GetName().Version?.ToString() ?? "dev";
        options.Labels["env"] = builder.Environment.EnvironmentName.ToLowerInvariant();
        options.Labels["app"] = "unified-sample";
    });
builder.Services.AddKubeJobDashboard(routePrefix: "admin/jobs");

var app = builder.Build();
app.UseStaticFiles();
app.UseRouting();
app.MapControllers();
app.MapGet("/", context =>
{
    context.Response.Redirect("/admin/jobs");
    return Task.CompletedTask;
});
app.Run();
