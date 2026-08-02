using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Npgsql;

namespace KubeJob.Benchmark;

/// <summary>
/// Background sampler that probes external telemetry while the pipeline runs.
/// It never touches the control-plane code path, so it cannot inflate pipeline
/// throughput. Three signals are collected:
/// <list type="bullet">
/// <item>PostgreSQL connection count via <c>pg_stat_activity</c> (always on).</item>
/// <item>RabbitMQ <c>messages_ready</c> + <c>messages_unacknowledged</c> via the
/// management HTTP API (the dev compose enables the plugin on 15672).</item>
/// <item>PostgreSQL CPU via best-effort <c>podman stats --no-stream</c>; skipped
/// silently when podman or the container is unavailable.</item>
/// </list>
/// </summary>
public sealed class MetricsSampler : IAsyncDisposable
{
    private readonly string _dbName;
    private readonly string _managementUri;
    private readonly string _basicAuth;
    private readonly IReadOnlyList<string> _queueNames;
    private readonly string? _containerName;
    private readonly TimeSpan _interval;
    private readonly HttpClient _http;
    private readonly NpgsqlDataSource _dataSource;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    // Aggregations, updated only by the sampler loop and read after Stop.
    private int _maxConnections;
    private int _maxReady;
    private int _maxUnacked;
    private double _cpuSum;
    private int _cpuSamples;
    private int _sampleCount;
    private long _maxProcessMemoryBytes;
    private double _processMemorySum;
    private int _processMemorySamples;
    private readonly long _processStartMemoryBytes;
    private readonly long _allocatedBaseline;
    private readonly int _gen0Baseline;
    private readonly int _gen1Baseline;
    private readonly int _gen2Baseline;
    private int _maxProcessThreads;
    private int _maxThreadPoolThreads;
    private long _maxWorkingSetBytes;

    public MetricsSampler(
        string benchDbConnStr,
        string managementUri,
        string rabbitUser,
        string rabbitPassword,
        IReadOnlyList<string> queueNames,
        string? containerName,
        TimeSpan interval)
    {
        // Tag the sampler's own connection so it can be excluded from the count.
        var builder = new NpgsqlConnectionStringBuilder(benchDbConnStr)
        {
            ApplicationName = "kubejob-bench-metrics"
        };
        var dbConnStrWithAppName = builder.ConnectionString;
        _dbName = builder.Database ?? string.Empty;
        _managementUri = managementUri.TrimEnd('/');
        _basicAuth = Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes($"{rabbitUser}:{rabbitPassword}"));
        _queueNames = queueNames;
        _containerName = string.IsNullOrWhiteSpace(containerName) ? null : containerName;
        _interval = interval;
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        // Own the pool so it can be closed deterministically before the bench
        // database is dropped; a process-wide pool would leave idle connections
        // open and block DROP DATABASE.
        _dataSource = NpgsqlDataSource.Create(dbConnStrWithAppName);
        _processStartMemoryBytes = GC.GetTotalMemory(forceFullCollection: false);
        _allocatedBaseline = GC.GetTotalAllocatedBytes(precise: false);
        _gen0Baseline = GC.CollectionCount(0);
        _gen1Baseline = GC.CollectionCount(1);
        _gen2Baseline = GC.CollectionCount(2);
        _loop = Task.Run(SampleLoopAsync);
    }

    private async Task SampleLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try { await SampleOnceAsync(); }
            catch
            {
                // A single bad sample must never abort the whole run; the next
                // tick will retry.
            }
            _sampleCount++;
            try { await Task.Delay(_interval, _cts.Token); }
            catch (TaskCanceledException) { break; }
        }
    }

    private async Task SampleOnceAsync()
    {
        // Process-local sampling first: it cannot fail and must not be skipped
        // when an external probe (DB/broker) throws.
        SampleProcessMemory();
        SampleThreads();
        await SampleDbConnectionsAsync();
        await SampleRabbitQueuesAsync();
        if (_containerName is not null)
        {
            SampleCpu();
        }
    }

    private void SampleThreads()
    {
        UpdateMax(ref _maxProcessThreads, Process.GetCurrentProcess().Threads.Count);
        UpdateMax(ref _maxThreadPoolThreads, ThreadPool.ThreadCount);
    }

    private async Task SampleDbConnectionsAsync()
    {
        if (string.IsNullOrEmpty(_dbName)) return;
        await using var conn = await _dataSource.OpenConnectionAsync(_cts.Token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT count(*) FROM pg_stat_activity " +
            "WHERE datname = @db AND application_name <> @app";
        cmd.Parameters.AddWithValue("db", _dbName);
        cmd.Parameters.AddWithValue("app", "kubejob-bench-metrics");
        var count = (long)(await cmd.ExecuteScalarAsync(_cts.Token) ?? 0);
        // Interlocked update of the max.
        int current;
        do
        {
            current = _maxConnections;
            if (count <= current) break;
        } while (Interlocked.CompareExchange(ref _maxConnections, (int)count, current) != current);
    }

    private async Task SampleRabbitQueuesAsync()
    {
        var ready = 0;
        var unacked = 0;
        foreach (var queue in _queueNames)
        {
            if (string.IsNullOrEmpty(queue)) continue;
            // vhost "/" is URL-encoded as %2F; the queue name is fully escaped.
            var url = $"{_managementUri}/api/queues/%2F/{Uri.EscapeDataString(queue)}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", _basicAuth);
            try
            {
                using var response = await _http.SendAsync(request, _cts.Token);
                if (!response.IsSuccessStatusCode) continue; // 404 before the queue is declared
                await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
                using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);
                if (doc.RootElement.TryGetProperty("messages_ready", out var r) && r.TryGetInt64(out var rv))
                    ready += (int)rv;
                if (doc.RootElement.TryGetProperty("messages_unacknowledged", out var u) && u.TryGetInt64(out var uv))
                    unacked += (int)uv;
            }
            catch (OperationCanceledException) { throw; }
            catch
            {
                // Management API not ready or transient; skip this queue.
            }
        }

        UpdateMax(ref _maxReady, ready);
        UpdateMax(ref _maxUnacked, unacked);
    }

    /// <summary>
    /// Sample the managed process memory using GC.GetTotalMemory.
    /// </summary>
    private void SampleProcessMemory()
    {
        // Two memory views: managed heap (GC.GetTotalMemory) and the full
        // process working set (native + managed, the number top/ps reports).
        var bytes = GC.GetTotalMemory(forceFullCollection: false);
        // Track max.
        long currentMax;
        do
        {
            currentMax = _maxProcessMemoryBytes;
            if (bytes <= currentMax) break;
        } while (Interlocked.CompareExchange(ref _maxProcessMemoryBytes, bytes, currentMax) != currentMax);
        AddAtomic(ref _processMemorySum, (double)bytes);
        Interlocked.Increment(ref _processMemorySamples);

        var workingSet = Process.GetCurrentProcess().WorkingSet64;
        long currentWs;
        do
        {
            currentWs = _maxWorkingSetBytes;
            if (workingSet <= currentWs) break;
        } while (Interlocked.CompareExchange(ref _maxWorkingSetBytes, workingSet, currentWs) != currentWs);
    }

    /// <summary>
    /// Best-effort CPU sampling via <c>podman stats --no-stream --format</c>.
    /// Failures (podman missing, container down, parse error) are silent and
    /// contribute no sample; the reported value is the average over successful
    /// samples only.
    /// </summary>
    private void SampleCpu()
    {
        var psi = new ProcessStartInfo
        {
            FileName = "podman",
            Arguments = $"stats --no-stream --format \"{{{{.CPUPerc}}}}\" \"{_containerName}\"",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        using var process = Process.Start(psi);
        if (process is null) return;
        if (!process.WaitForExit(8000))
        {
            try { process.Kill(); } catch { /* ignore */ }
            return;
        }
        if (process.ExitCode != 0) return;
        var line = process.StandardOutput.ReadLine();
        if (string.IsNullOrWhiteSpace(line)) return;
        line = line.Trim().TrimEnd('%');
        if (double.TryParse(line, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var pct))
        {
            AddAtomic(ref _cpuSum, pct);
            Interlocked.Increment(ref _cpuSamples);
        }
    }

    private static void UpdateMax(ref int target, int candidate)
    {
        int current;
        do
        {
            current = target;
            if (candidate <= current) break;
        } while (Interlocked.CompareExchange(ref target, candidate, current) != current);
    }

    private static void AddAtomic(ref double target, double add)
    {
        double current;
        do
        {
            current = target;
        } while (Interlocked.CompareExchange(ref target, current + add, current) != current);
    }

    public MetricSamples Snapshot()
    {
        var allocated = GC.GetTotalAllocatedBytes(precise: false) - _allocatedBaseline;
        return new MetricSamples(
            _maxConnections,
            _maxReady,
            _maxUnacked,
            _cpuSamples == 0 ? 0 : _cpuSum / _cpuSamples,
            _sampleCount,
            _maxProcessMemoryBytes,
            _processMemorySamples == 0 ? 0 : (_processMemorySum / _processMemorySamples),
            _processStartMemoryBytes,
            allocated,
            GC.CollectionCount(0) - _gen0Baseline,
            GC.CollectionCount(1) - _gen1Baseline,
            GC.CollectionCount(2) - _gen2Baseline,
            _maxProcessThreads,
            _maxThreadPoolThreads,
            _maxWorkingSetBytes);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _loop; } catch { /* sampler loop ends on cancel */ }
        _cts.Dispose();
        _http.Dispose();
        await _dataSource.DisposeAsync();
    }
}