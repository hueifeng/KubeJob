using System.Collections;
using KubeJob.Benchmark;

// Top-level entry point for the KubeJob throughput benchmark. Re-runnable:
//   dotnet run --project tests/KubeJob.Benchmark -- --jobs 5000 --scenarios Parallel,KeyOrderedHotKey
//   dotnet run --project tests/KubeJob.Benchmark -- --runtime BrokerNative --jobs 5000
// Every parameter also has an environment-variable form; see README.md.

var env = Environment.GetEnvironmentVariables()
    .Cast<DictionaryEntry>()
    .ToDictionary(entry => (string)entry.Key, entry => (string?)entry.Value ?? string.Empty);

var opts = BenchmarkOptions.Parse(env, args);
var runtime = ResolveRuntime(env, args);

if (runtime == "BrokerNative")
{
    BrokerNativeResultTable.PrintHeader(opts);
    try
    {
        var result = await new BrokerNativePipelineBenchmark(opts).RunAsync();
        BrokerNativeResultTable.Print(result);
        var markdown = BrokerNativeResultTable.ToMarkdown(opts, result);
        Console.WriteLine("--- markdown ---");
        Console.WriteLine(markdown);

        if (!string.IsNullOrWhiteSpace(opts.OutputFile))
        {
            File.WriteAllText(opts.OutputFile, markdown);
            Console.WriteLine($"Wrote {opts.OutputFile}");
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"BrokerNative benchmark failed: {ex}");
        Environment.ExitCode = 1;
    }

    return;
}

ResultTable.PrintHeader(opts);

var bench = new PipelineBenchmark(opts);
var results = new List<ScenarioResult>();

foreach (var scenario in opts.Scenarios)
{
    Console.WriteLine($"=== {scenario.Label()} ({opts.SubmissionMode}) ===");
    var sw = System.Diagnostics.Stopwatch.StartNew();
    try
    {
        var scenarioResults = await bench.RunScenarioAsync(scenario);
        results.AddRange(scenarioResults);
        foreach (var result in scenarioResults)
            ResultTable.PrintRow(result);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"  !! Scenario {scenario.Label()} failed after {sw.Elapsed.TotalSeconds:F1}s: {ex}");
    }
}

if (results.Count > 0)
{
    var markdown = ResultTable.ToMarkdown(opts, results);
    Console.WriteLine();
    Console.WriteLine("--- markdown ---");
    Console.WriteLine(markdown);

    if (!string.IsNullOrWhiteSpace(opts.OutputFile))
    {
        File.WriteAllText(opts.OutputFile, markdown);
        Console.WriteLine($"Wrote {opts.OutputFile}");
    }
}
else
{
    Console.Error.WriteLine("No scenarios completed successfully.");
}

static string ResolveRuntime(IDictionary<string, string> env, string[] args)
{
    for (var index = 0; index + 1 < args.Length; index++)
    {
        if (string.Equals(args[index], "--runtime", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(args[index + 1], "BrokerNative", StringComparison.OrdinalIgnoreCase)
                ? "BrokerNative"
                : "PostgresManaged";
        }
    }

    return env.TryGetValue("KUBEJOB_BENCH_RUNTIME", out var value)
           && string.Equals(value, "BrokerNative", StringComparison.OrdinalIgnoreCase)
        ? "BrokerNative"
        : "PostgresManaged";
}
