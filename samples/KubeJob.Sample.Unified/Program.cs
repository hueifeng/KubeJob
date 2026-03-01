using KubeJob.Server.Extensions;
using KubeJob.Worker.Extensions;
using KubeJob.Sample.WorkerNode.Jobs;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;

var builder = WebApplication.CreateBuilder(args);

// 1. Add KubeJob Server (Control Plane + Dashboard)
// Mount UI to a custom route like "/admin/jobs" instead of default "/kubejob"
builder.Services.AddKubeJobServer(opts => opts.UseInMemory());
builder.Services.AddKubeJobDashboard(routePrefix: "/admin/jobs");

// 2. Add KubeJob Worker (Data Plane)
// In a unified setup, Server and Worker share the same process!
builder.Services.AddKubeJobWorker(options => 
{
    // Point back to itself using the local Kestrel port
    options.ServerEndpoint = "http://localhost:5041"; 
    options.MaxConcurrentJobs = 10;
    options.Labels.Add("env", "dev");
    options.Labels.Add("app", "unified");
});

// Register the specific jobs
builder.Services.AddTransient<SampleDataJob>();
builder.Services.AddTransient<FailingJob>();
builder.Services.AddTransient<LongRunningJob>();
builder.Services.AddTransient<BroadcastJob>();

var app = builder.Build();

// 3. Initialize DB Schema
app.InitializeKubeJobDatabase();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
}
app.UseStaticFiles();

// Make sure routing is set up
app.UseRouting();
app.MapControllers();

// Add a root redirect to our custom dashboard route
app.MapGet("/", context => {
    context.Response.Redirect("/admin/jobs");
    return Task.CompletedTask;
});

app.Run("http://localhost:5041");