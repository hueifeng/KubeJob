using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Core.Attributes;
using KubeJob.Core.Context;
using KubeJob.Core.Domain;
using KubeJob.Core.Dtos;
using KubeJob.Core.Interfaces;
using KubeJob.Worker.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KubeJob.Worker.Services
{
    public class WorkerAgentService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<WorkerAgentService> _logger;
        private readonly KubeJobWorkerOptions _options;
        private readonly HttpClient _httpClient;
        private readonly string _workerId;
        private readonly ConcurrentDictionary<string, Task> _runningJobs = new();

        public WorkerAgentService(
            IServiceProvider serviceProvider,
            ILogger<WorkerAgentService> logger,
            IOptions<KubeJobWorkerOptions> options)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options.Value;
            _httpClient = new HttpClient { BaseAddress = new Uri(_options.ServerEndpoint) };
            
            // Allow fixing the worker ID in options to prevent a new node on every restart
            _workerId = string.IsNullOrWhiteSpace(_options.WorkerId) 
                ? $"{Environment.MachineName}-{Guid.NewGuid().ToString().Substring(0, 8)}"
                : _options.WorkerId;
        }

        private string GetLocalIpAddress()
        {
            try
            {
                var host = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName());
                foreach (var ip in host.AddressList)
                {
                    if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    {
                        return ip.ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to resolve local IPv4 address, falling back to loopback.");
            }
            return "127.0.0.1";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("KubeJob Worker {WorkerId} started. Connecting to {Endpoint}", _workerId, _options.ServerEndpoint);

            // Register discovered jobs to server automatically
            await RegisterJobsAsync(stoppingToken);

            var heartbeatTask = HeartbeatLoopAsync(stoppingToken);
            var pollTask = PollLoopAsync(stoppingToken);

            await Task.WhenAll(heartbeatTask, pollTask);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker {WorkerId} is shutting down...", _workerId);
            
            if (!_runningJobs.IsEmpty)
            {
                _logger.LogInformation("Waiting for {Count} running jobs to finish or timeout...", _runningJobs.Count);
                var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                var allJobsTask = Task.WhenAll(_runningJobs.Values);
                
                await Task.WhenAny(allJobsTask, timeoutTask);
                
                if (allJobsTask.IsCompleted)
                {
                    _logger.LogInformation("All running jobs finished gracefully.");
                }
                else
                {
                    _logger.LogWarning("Some jobs did not finish within the shutdown timeout. They will be re-queued by the Server's NodeHealthService.");
                }
            }

            await base.StopAsync(cancellationToken);
        }

        private async Task RegisterJobsAsync(CancellationToken token)
        {
            try
            {
                var jobTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(GetLoadableTypes)
                    .Where(t => typeof(IKubeJob).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

                var req = new RegisterJobsRequest { WorkerId = _workerId };

                foreach (var type in jobTypes)
                {
                    var attr = type.GetCustomAttribute<KubeJobAttribute>();
                    if (attr == null) continue;

                    var nodeSelectors = type.GetCustomAttributes<NodeSelectorAttribute>()
                        .ToDictionary(a => a.Key, a => a.Value);

                    req.Jobs.Add(new JobRegistrationDto
                    {
                        Name = string.IsNullOrWhiteSpace(attr.Name) ? type.Name : attr.Name,
                        Cron = attr.Cron,
                        ExecuteModel = attr.ExecuteModel,
                        TotalShards = attr.TotalShards,
                        TimeoutSeconds = attr.TimeoutSeconds,
                        MaxRetries = attr.MaxRetries,
                        NodeSelectors = nodeSelectors
                    });
                }

                if (req.Jobs.Any())
                {
                    _logger.LogInformation("Registering {Count} job types to server...", req.Jobs.Count);
                    
                    bool registered = false;
                    while (!registered && !token.IsCancellationRequested)
                    {
                        try
                        {
                            var response = await _httpClient.PostAsJsonAsync("/api/kubejob/worker/register", req, token);
                            response.EnsureSuccessStatusCode();
                            _logger.LogInformation("Successfully registered jobs to server.");
                            registered = true;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning("Failed to register jobs to server (will retry in 2s): {Message}", ex.Message);
                            await Task.Delay(2000, token);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to register jobs to server: {Message}", ex.Message);
            }
        }

        private async Task HeartbeatLoopAsync(CancellationToken stoppingToken)
        {
            var ipAddress = GetLocalIpAddress();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var req = new HeartbeatRequest
                    {
                        WorkerId = _workerId,
                        IpAddress = ipAddress,
                        Labels = _options.Labels,
                        CurrentLoad = _runningJobs.Count,
                        MaxCapacity = _options.MaxConcurrentJobs,
                        RunningJobIds = _runningJobs.Keys.ToList()
                    };

                    var response = await _httpClient.PostAsJsonAsync("/api/kubejob/worker/heartbeat", req, stoppingToken);
                    response.EnsureSuccessStatusCode();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to send heartbeat: {Message}", ex.Message);
                }

                await Task.Delay(5000, stoppingToken);
            }
        }

        private async Task PollLoopAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (_runningJobs.Count >= _options.MaxConcurrentJobs)
                    {
                        await Task.Delay(2000, stoppingToken);
                        continue;
                    }

                    var response = await _httpClient.GetAsync($"/api/kubejob/worker/poll?workerId={_workerId}", stoppingToken);
                    if (response.IsSuccessStatusCode)
                    {
                        var pollResult = await response.Content.ReadFromJsonAsync<PollJobsResponse>(cancellationToken: stoppingToken);
                        if (pollResult != null && pollResult.Jobs.Any())
                        {
                            foreach (var run in pollResult.Jobs)
                            {
                                if (!_runningJobs.ContainsKey(run.Id))
                                {
                                    // Start job execution in background
                                    var execTask = ExecuteJobSandboxAsync(run, stoppingToken);
                                    _runningJobs.TryAdd(run.Id, execTask);
                                    
                                    // Fire and forget cleanup
                                    _ = execTask.ContinueWith(t => _runningJobs.TryRemove(run.Id, out _));
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to poll jobs: {Message}", ex.Message);
                    await Task.Delay(5000, stoppingToken); // Backoff on error
                }
            }
        }

        private async Task ExecuteJobSandboxAsync(JobRun run, CancellationToken systemToken)
        {
            _logger.LogInformation("Starting execution of JobRun {RunId}", run.Id);
            
            // Report Running
            await ReportStatusAsync(run.Id, Core.Enums.JobStatus.Running);

            try
            {
                using var scope = _serviceProvider.CreateScope();
                
                // Lookup type by run.JobType (provided by server)
                var resolvedJobType = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(GetLoadableTypes)
                    .FirstOrDefault(t => t.Name == run.JobType || (t.GetCustomAttribute<KubeJobAttribute>()?.Name == run.JobType));

                if (resolvedJobType == null || !typeof(IKubeJob).IsAssignableFrom(resolvedJobType))
                {
                    throw new Exception($"Cannot find IKubeJob implementation for JobType: {run.JobType}");
                }

                var jobInstance = (IKubeJob)scope.ServiceProvider.GetRequiredService(resolvedJobType);
                var context = new KubeJobContext
                {
                    RunId = run.Id,
                    SpecId = run.SpecId,
                    BatchId = run.BatchId,
                    ShardIndex = run.ShardIndex,
                    TotalShards = 1, 
                    ServiceProvider = scope.ServiceProvider,
                    Logger = scope.ServiceProvider.GetRequiredService<ILogger<IKubeJob>>()
                };

                // Apply K8s inspired ActiveDeadlineSeconds (TimeoutSeconds)
                var timeoutSecs = run.TimeoutSeconds > 0 ? run.TimeoutSeconds : 300;
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSecs));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(systemToken, timeoutCts.Token);

                await jobInstance.ExecuteAsync(context, linkedCts.Token);

                // Report Success
                await ReportStatusAsync(run.Id, Core.Enums.JobStatus.Succeeded, "Execution completed successfully");
                _logger.LogInformation("Completed execution of JobRun {RunId}", run.Id);
            }
            catch (OperationCanceledException)
            {
                await ReportStatusAsync(run.Id, Core.Enums.JobStatus.Failed, "Job timed out or canceled (ActiveDeadlineSeconds reached)");
                _logger.LogError("JobRun {RunId} timed out", run.Id);
            }
            catch (Exception ex)
            {
                await ReportStatusAsync(run.Id, Core.Enums.JobStatus.Failed, ex.ToString());
                _logger.LogError(ex, "Failed execution of JobRun {RunId}", run.Id);
            }
        }

        private async Task ReportStatusAsync(string runId, Core.Enums.JobStatus status, string msg = "")
        {
            var req = new JobReportRequest
            {
                WorkerId = _workerId,
                RunId = runId,
                Status = status,
                ResultMsg = msg
            };
            try
            {
                await _httpClient.PostAsJsonAsync("/api/kubejob/worker/report", req);
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Failed to report status for {RunId}", runId);
            }
        }

        private IEnumerable<Type> GetLoadableTypes(Assembly assembly)
        {
            try
            {
                return assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                _logger.LogWarning(ex, "Partial type load failure in assembly {Assembly}.", assembly.FullName);
                return ex.Types.Where(t => t != null).Cast<Type>();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to scan types in assembly {Assembly}.", assembly.FullName);
                return Array.Empty<Type>();
            }
        }
    }
}
