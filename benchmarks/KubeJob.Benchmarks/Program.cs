using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

BenchmarkRunner.Run<PayloadEncodingBenchmark>();

[MemoryDiagnoser]
public class PayloadEncodingBenchmark
{
    private readonly string _payload = "{\"value\":42,\"name\":\"benchmark\"}";

    [Benchmark]
    public byte[] Utf8Encoding() => System.Text.Encoding.UTF8.GetBytes(_payload);

    [Benchmark]
    public int Utf8ByteCount() => System.Text.Encoding.UTF8.GetByteCount(_payload);
}
