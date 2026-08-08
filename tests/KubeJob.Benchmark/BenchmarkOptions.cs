using System.Globalization;
using KubeJob.Core.Runtime;

namespace KubeJob.Benchmark;

/// <summary>
/// End-to-end V3 benchmark options. The primary comparison is execution
/// authority: PostgresManaged versus BrokerNative. RabbitMQ-specific admission,
/// lane and dispatcher knobs were removed with the legacy dual-authority path.
/// </summary>
public sealed class BenchmarkOptions
{
    public string PostgresConnectionString { get; set; } =
        "Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres";
    public string RabbitMqConnectionString { get; set; } =
        "amqp://kubejob:kubejob-dev@localhost:5672/";
    public string RabbitMqManagementUri { get; set; } = "http://localhost:15672";
    public string RabbitMqUser { get; set; } = "kubejob";
    public string RabbitMqPassword { get; set; } = "kubejob-dev";
    public string PostgresContainerName { get; set; } = "kubejob-dev-postgres-1";
    public bool CpuSamplingEnabled { get; set; } = true;

    public QueueRuntimeMode RuntimeMode { get; set; } = QueueRuntimeMode.BrokerNative;

    public int JobCount { get; set; } = 10_000;
    public int Warmup { get; set; } = 500;
    public int JobWorkMs { get; set; }
    public int SubmitterConcurrency { get; set; } = 32;
    public int WorkerMaxConcurrency { get; set; } = 128;
    public int PrefetchCount { get; set; } = 256;

    // PostgresManaged tuning only.
    public int OutboxPublishConcurrency { get; set; } = 4;
    public int OutboxBatchSize { get; set; } = 512;
    public int OutboxPollIntervalMs { get; set; } = 10;

    public int RunTimeoutSeconds { get; set; } = 180;
    public int HotKeyCardinality { get; set; } = 4;
    public int UniformKeyCardinality { get; set; }
    public int MetricsIntervalMs { get; set; } = 1000;

    public IReadOnlyList<BenchScenario> Scenarios { get; set; } =
        new[] { BenchScenario.Parallel };

    public string? OutputFile { get; set; }

    public static BenchmarkOptions Parse(IDictionary<string, string> env, string[] args)
    {
        var opts = new BenchmarkOptions
        {
            PostgresConnectionString = Env(env, "KUBEJOB_BENCHMARK_POSTGRES",
                "KUBEJOB_TEST_POSTGRES",
                "Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres"),
            RabbitMqConnectionString = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ",
                "KUBEJOB_RABBITMQ_TEST_CONNECTION",
                "amqp://kubejob:kubejob-dev@localhost:5672/"),
            RabbitMqManagementUri = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_MANAGEMENT", "http://localhost:15672"),
            RabbitMqUser = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_USER", "kubejob"),
            RabbitMqPassword = Env(env, "KUBEJOB_BENCHMARK_RABBITMQ_PASSWORD", "kubejob-dev"),
            PostgresContainerName = Env(env, "KUBEJOB_BENCH_POSTGRES_CONTAINER", "kubejob-dev-postgres-1")
        };

        opts.CpuSamplingEnabled = EnvBool(env, "KUBEJOB_BENCH_CPU", opts.CpuSamplingEnabled);
        opts.RuntimeMode = ParseRuntime(Env(env, "KUBEJOB_BENCH_RUNTIME", opts.RuntimeMode.ToString()));
        opts.JobCount = EnvInt(env, "KUBEJOB_BENCH_JOBS", opts.JobCount);
        opts.Warmup = EnvInt(env, "KUBEJOB_BENCH_WARMUP", opts.Warmup);
        opts.JobWorkMs = EnvInt(env, "KUBEJOB_BENCH_WORK_MS", opts.JobWorkMs);
        opts.SubmitterConcurrency = EnvInt(env, "KUBEJOB_BENCH_SUBMITTERS", opts.SubmitterConcurrency);
        opts.WorkerMaxConcurrency = EnvInt(env, "KUBEJOB_BENCH_WORKER_CONCURRENCY", opts.WorkerMaxConcurrency);
        opts.PrefetchCount = EnvInt(env, "KUBEJOB_BENCH_PREFETCH", opts.PrefetchCount);
        opts.OutboxPublishConcurrency = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_CONCURRENCY", opts.OutboxPublishConcurrency);
        opts.OutboxBatchSize = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_BATCH", opts.OutboxBatchSize);
        opts.OutboxPollIntervalMs = EnvInt(env, "KUBEJOB_BENCH_OUTBOX_POLL_MS", opts.OutboxPollIntervalMs);
        opts.RunTimeoutSeconds = EnvInt(env, "KUBEJOB_BENCH_RUN_TIMEOUT_S", opts.RunTimeoutSeconds);
        opts.HotKeyCardinality = EnvInt(env, "KUBEJOB_BENCH_HOTKEY_COUNT", opts.HotKeyCardinality);
        opts.UniformKeyCardinality = EnvInt(env, "KUBEJOB_BENCH_UNIFORM_KEYS", opts.UniformKeyCardinality);
        opts.MetricsIntervalMs = EnvInt(env, "KUBEJOB_BENCH_METRICS_MS", opts.MetricsIntervalMs);

        var scenarioEnv = Env(env, "KUBEJOB_BENCH_SCENARIOS", string.Empty);
        if (!string.IsNullOrWhiteSpace(scenarioEnv))
        {
            opts.Scenarios = ParseScenarios(scenarioEnv);
        }

        ApplyArgs(opts, args);
        Validate(opts);
        return opts;
    }

    private static void Validate(BenchmarkOptions opts)
    {
        if (opts.JobCount < 1 || opts.Warmup < 0 || opts.SubmitterConcurrency < 1
            || opts.WorkerMaxConcurrency < 1 || opts.PrefetchCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(opts), "Benchmark counts and concurrency values must be positive.");
        }

        if (opts.RuntimeMode == QueueRuntimeMode.BrokerNative
            && opts.Scenarios.Any(s => s != BenchScenario.Parallel))
        {
            throw new NotSupportedException(
                "BrokerNative benchmark currently supports Parallel only. " +
                "PartitionKey/transport-native ordering must be benchmarked after that feature is enabled.");
        }
    }

    private static QueueRuntimeMode ParseRuntime(string value) =>
        Enum.TryParse<QueueRuntimeMode>(value, ignoreCase: true, out var parsed)
            ? parsed
            : throw new ArgumentException($"Unknown KubeJob runtime mode '{value}'.");

    private static string Env(IDictionary<string, string> env, string key, string fallback) =>
        env.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : fallback;

    private static string Env(
        IDictionary<string, string> env,
        string primary,
        string secondary,
        string fallback) =>
        Env(env, primary, Env(env, secondary, fallback));

    private static int EnvInt(IDictionary<string, string> env, string key, int fallback) =>
        env.TryGetValue(key, out var value)
        && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;

    private static bool EnvBool(IDictionary<string, string> env, string key, bool fallback)
    {
        if (!env.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return !(value.Equals("0", StringComparison.Ordinal)
                 || value.Equals("false", StringComparison.OrdinalIgnoreCase)
                 || value.Equals("no", StringComparison.OrdinalIgnoreCase)
                 || value.Equals("off", StringComparison.OrdinalIgnoreCase));
    }

    private static IReadOnlyList<BenchScenario> ParseScenarios(string csv) =>
        csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(value => Enum.Parse<BenchScenario>(value, ignoreCase: true))
            .ToArray();

    private static void ApplyArgs(BenchmarkOptions opts, string[] args)
    {
        for (var index = 0; index < args.Length; index++)
        {
            var token = args[index];
            if (!token.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            var key = token[2..];
            var value = index + 1 < args.Length ? args[++index] : null;
            if (value is null)
            {
                continue;
            }

            switch (key)
            {
                case "runtime": opts.RuntimeMode = ParseRuntime(value); break;
                case "jobs": opts.JobCount = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "warmup": opts.Warmup = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "work-ms": opts.JobWorkMs = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "submitters": opts.SubmitterConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "worker-concurrency": opts.WorkerMaxConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "prefetch": opts.PrefetchCount = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "outbox-concurrency": opts.OutboxPublishConcurrency = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "outbox-batch": opts.OutboxBatchSize = int.Parse(value, CultureInfo.InvariantCulture); break;
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
            }
        }
    }
}
