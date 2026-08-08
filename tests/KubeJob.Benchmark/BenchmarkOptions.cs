using System.Globalization;

namespace KubeJob.Benchmark;

/// <summary>
/// All knobs for one benchmark run. Defaults match the Podman dev stack in
/// <c>compose.yaml</c> (PostgreSQL <c>kubejob/kubejob-dev</c> on 5432, RabbitMQ
/// <c>kubejob/kubejob-dev</c> on 5672 with the management plugin on 15672). Every
/// value can be overridden by an environment variable or a <c>--key value</c>
/// command-line argument; arguments take precedence over environment, which
/// takes precedence over the built-in default.
/// </summary>
public sealed class BenchmarkOptions
{
    // --- External endpoints ---
    public string PostgresConnectionString { get; set; } =
        "Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres";
    public string RabbitMqConnectionString { get; set; } =
        "amqp://kubejob:kubejob-dev@localhost:5672/";
    public string RabbitMqManagementUri { get; set; } = "http://localhost:15672";
    public string RabbitMqUser { get; set; } = "kubejob";
    public string RabbitMqPassword { get; set; } = "kubejob-dev";

    // --- PostgreSQL durability ---
    /// <summary>
    /// Keeps PostgreSQL's normal durable commit semantics by default. Turning
    /// this off is an explicit throughput experiment and must not be presented
    /// as a production-durability result.
    /// </summary>
    public bool SynchronousCommitEnabled { get; set; } = true;

    // --- Best-effort metrics sources ---
    /// <summary>Podman container name for <c>podman stats</c> CPU sampling. Empty disables CPU.</summary>
    public string PostgresContainerName { get; set; } = "kubejob-dev-postgres-1";
    public bool CpuSamplingEnabled { get; set; } = true;

    // --- Workload ---
    public int JobCount { get; set; } = 2000;
    public int Warmup { get; set; } = 100;
    public int JobWorkMs { get; set; } = 0;

    // --- Submission driver ---
    /// <summary>Concurrent typed client calls or RabbitMQ ingress publishes.</summary>
    public int SubmitterConcurrency { get; set; } = 16;
    public SubmissionMode SubmissionMode { get; set; } = Benchmark.SubmissionMode.TypedClient;
    public int IngressBatchSize { get; set; } = 100;
    public int IngressBatchWaitMs { get; set; } = 5;
    public int IngressPrefetch { get; set; } = 200;

    // --- Worker ---
    public int WorkerMaxConcurrency { get; set; } = 128;

    // --- Server / outbox ---
    public int OutboxPublishConcurrency { get; set; } = 4;
    public int OutboxBatchSize { get; set; } = 512;
    public int OutboxPollIntervalMs { get; set; } = 10;

    /// <summary>Managed completion batcher flush window in milliseconds.</summary>
    public int CompletionFlushIntervalMs { get; set; } = 2;

    // --- Completion polling ---
    public int PollIntervalMs { get; set; } = 100;
    public int StatusPollParallelism { get; set; } = 32;
    public int RunTimeoutSeconds { get; set; } = 180;

    // --- Scenarios ---
    public IReadOnlyList<BenchScenario> Scenarios { get; set; } =
        new[] { BenchScenario.Parallel, BenchScenario.KeyOrderedUniform, BenchScenario.KeyOrderedHotKey };
    public int HotKeyCardinality { get; set; } = 4;
    /// <summary>Uniform key space size. Zero means a distinct key per Run.</summary>
    public int UniformKeyCardinality { get; set; } = 0;

    // --- Metrics sampling ---
    public int MetricsIntervalMs { get; set; } = 1000;

    // --- Output ---
    public string? OutputFile { get; set; }

    /// <summary>
    /// Parses environment variables and <c>--key value</c> arguments. Unknown
    /// keys are ignored so a caller can pass through profiling flags. Boolean
    /// values accept <c>0</c>/<c>false</c>/<c>no</c> as false.
    /// </summary>
    public static BenchmarkOptions Parse(IDictionary<string, string> env, string[] args)
    {
        var opts = new BenchmarkOptions();

        // Environment defaults first.
        opts.PostgresConnectionString = Env(env, "KUBEJOB_BENCHMARK_POSTGRES",
            "KUBEJOB_TEST_POSTGRES", opts.PostgresConnectionString);
        opts.RabbitMqConnectionString = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ",
            "KUBEJOB_RABBITMQ_TEST_CONNECTION", opts.RabbitMqConnectionString);
        opts.RabbitMqManagementUri = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_MANAGEMENT",
            opts.RabbitMqManagementUri);
        opts.RabbitMqUser = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_USER", opts.RabbitMqUser);
        opts.RabbitMqPassword = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_PASSWORD", opts.RabbitMqPassword);
        opts.SynchronousCommitEnabled = EnvBool(
            env,
            "KUBEJOB_BENCH_SYNCHRONOUS_COMMIT",
            opts.SynchronousCommitEnabled);
        opts.PostgresContainerName = Env(env, "KUBEJOB_BENCH_POSTGRES_CONTAINER",
            opts.PostgresContainerName);
        opts.CpuSamplingEnabled = EnvBool(env, "KUBEJOB_BENCH_CPU", opts.CpuSamplingEnabled);

        opts.JobCount = EnvInt(env, "KUBEJOB_BENCH_JOBS", opts.JobCount);
        opts.Warmup = EnvInt(env, "KUBEJOB_BENCH_WARMUP", opts.Warmup);
        opts.JobWorkMs = EnvInt(env, "KUBEJOB_BENCH_WORK_MS", opts.JobWorkMs);
        opts.SubmitterConcurrency = EnvInt(env, "KUBEJOB_BENCH_SUBMITTERS", opts.SubmitterConcurrency);
        opts.SubmissionMode = Enum.TryParse(Env(env, "KUBEJOB_BENCH_MODE",
            opts.SubmissionMode.ToString()), ignoreCase: true, out SubmissionMode sm)
            ? sm : opts.SubmissionMode;
        opts.IngressBatchSize = EnvInt(env, "KUBEJOB_BENCH_INGRESS_BATCH", opts.IngressBatchSize);
        opts.IngressBatchWaitMs = EnvInt(env, "KUBEJOB_BENCH_INGRESS_WAIT_MS", opts.IngressBatchWaitMs);
        opts.IngressPrefetch = EnvInt(env, "KUBEJOB_BENCH_INGRESS_PREFETCH", opts.IngressPrefetch);
        opts.WorkerMaxConcurrency = EnvInt(env, "KUBEJOB_BENCH_WORKER_CONCURRENCY",
            opts.WorkerMaxConcurrency);
        opts.OutboxPublishConcurrency = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_CONCURRENCY",
            opts.OutboxPublishConcurrency);
        opts.OutboxBatchSize = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_BATCH", opts.OutboxBatchSize);
        opts.OutboxPollIntervalMs = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_POLL_MS",
            opts.OutboxPollIntervalMs);
        opts.CompletionFlushIntervalMs = EnvInt(env, "KUBEJOB_BENCH_COMPLETION_FLUSH_MS",
            opts.CompletionFlushIntervalMs);

        opts.PollIntervalMs = EnvInt(env, "KUBEJOB_BENCH_POLL_MS", opts.PollIntervalMs);
        opts.StatusPollParallelism = EnvInt(env, "KUBEJOB_BENCH_STATUS_PARALLELISM",
            opts.StatusPollParallelism);
        opts.RunTimeoutSeconds = EnvInt(env, "KUBEJOB_BENCH_RUN_TIMEOUT_S", opts.RunTimeoutSeconds);

        opts.HotKeyCardinality = EnvInt(env, "KUBEJOB_BENCH_HOTKEY_COUNT", opts.HotKeyCardinality);
        opts.UniformKeyCardinality = EnvInt(env, "KUBEJOB_BENCH_UNIFORM_KEYS", opts.UniformKeyCardinality);
        opts.MetricsIntervalMs = EnvInt(env, "KUBEJOB_BENCH_METRICS_MS", opts.MetricsIntervalMs);

        var scenarioEnv = Env(env, "KUBEJOB_BENCH_SCENARIOS", string.Empty);
        if (!string.IsNullOrWhiteSpace(scenarioEnv))
        {
            opts.Scenarios = ParseScenarios(scenarioEnv);
        }

        // --key value arguments override environment.
        ApplyArgs(opts, args);
        return opts;
    }

    private static string Env(IDictionary<string, string> env, string key, string fallback) =>
        env.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v : fallback;

    private static string Env(IDictionary<string, string> env, string primary, string secondary, string fallback) =>
        Env(env, primary, Env(env, secondary, fallback));

    private static int EnvInt(IDictionary<string, string> env, string key, int fallback) =>
        env.TryGetValue(key, out var v) && int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i)
            ? i : fallback;

    private static bool EnvBool(IDictionary<string, string> env, string key, bool fallback)
    {
        if (!env.TryGetValue(key, out var v) || string.IsNullOrWhiteSpace(v)) return fallback;
        return !(v.Equals("0", StringComparison.Ordinal) || v.Equals("false", StringComparison.OrdinalIgnoreCase)
                 || v.Equals("no", StringComparison.OrdinalIgnoreCase) || v.Equals("off", StringComparison.OrdinalIgnoreCase));
    }

    private static IReadOnlyList<BenchScenario> ParseScenarios(string csv) =>
        csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(s => Enum.Parse<BenchScenario>(s, ignoreCase: true))
            .ToArray();

    private static void ApplyArgs(BenchmarkOptions opts, string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (!token.StartsWith("--", StringComparison.Ordinal)) continue;
            var key = token[2..];
            var value = i + 1 < args.Length ? args[++i] : null;
            if (value is null) continue;
            switch (key)
            {
                case "jobs": opts.JobCount = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "warmup": opts.Warmup = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "work-ms": opts.JobWorkMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "submitters": opts.SubmitterConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "mode": opts.SubmissionMode = Enum.Parse<SubmissionMode>(value, ignoreCase: true); break;
                case "worker-concurrency": opts.WorkerMaxConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "outbox-concurrency": opts.OutboxPublishConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "outbox-batch": opts.OutboxBatchSize = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "completion-flush-ms": opts.CompletionFlushIntervalMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "poll-ms": opts.PollIntervalMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "status-parallelism": opts.StatusPollParallelism = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "run-timeout-s": opts.RunTimeoutSeconds = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "scenarios": opts.Scenarios = ParseScenarios(value); break;
                case "hotkey-count": opts.HotKeyCardinality = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "uniform-keys": opts.UniformKeyCardinality = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "metrics-ms": opts.MetricsIntervalMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "postgres": opts.PostgresConnectionString = value; break;
                case "rabbitmq": opts.RabbitMqConnectionString = value; break;
                case "rabbitmq-mgmt": opts.RabbitMqManagementUri = value; break;
                case "out": opts.OutputFile = value; break;
                case "cpu": opts.CpuSamplingEnabled = !value.Equals("0", StringComparison.Ordinal); break;
                case "container": opts.PostgresContainerName = value; break;
                case "synchronous-commit": opts.SynchronousCommitEnabled = ParseOnOff(value); break;
                default:
                    // Unknown flags are ignored so callers can pass profiling hints.
                    break;
            }
        }
    }

    private static bool ParseOnOff(string value) =>
        !(value.Equals("0", StringComparison.Ordinal)
          || value.Equals("false", StringComparison.OrdinalIgnoreCase)
          || value.Equals("no", StringComparison.OrdinalIgnoreCase)
          || value.Equals("off", StringComparison.OrdinalIgnoreCase));
}

/// <summary>
/// Submission entry point. <see cref="TypedClient"/> drives the production .NET
/// client (<c>IJobClient.EnqueueAsync</c>); <see cref="Ingress"/> publishes
/// RabbitMQ business messages through the ingress micro-batcher.
/// </summary>
public enum SubmissionMode
{
    TypedClient,
    Ingress
}
