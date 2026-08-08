using System.Collections;
using KubeJob.Benchmark;

// Top-level entry point for the KubeJob V3 throughput benchmark. Examples:
//   dotnet run --project tests/KubeJob.Benchmark -- --runtime BrokerNative --jobs 50000
//   dotnet run --project tests/KubeJob.Benchmark -- --runtime PostgresManaged --jobs 50000 --scenarios Parallel,KeyOrderedHotKey
// Every parameter also has an environment-variable form; see README.md.

var env = Environment.GetEnvironmentVariables()
    .Cast<DictionaryEntry>()
    .ToDictionary(entry => (string)entry.Key, entry => (string?)entry.Value ?? string.Empty);

var opts = BenchmarkOptions.Parse(env, args);
ResultTable.PrintHeader(opts);

var bench = new PipelineBenchmark(opts);
var results = new List<ScenarioResult>();

foreach (var scenario in opts.Scenarios)
{
    Console.WriteLine($"=== {scenario.Label()} ({opts.RuntimeMode}) ===");
    var sw = System.Diagnostics.Stopwatch.StartNew();
    try
    {
        var scenarioResults = await bench.RunScenarioAsync(scenario);
        results.AddRange(scenarioResults);
        foreach (var result in scenarioResults)
        {
            ResultTable.PrintRow(result);
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(
            $"  !! Scenario {scenario.Label()} failed after {sw.Elapsed.TotalSeconds:F1}s: {ex}");
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
