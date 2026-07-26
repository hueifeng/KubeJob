namespace KubeJob.Storage.PostgreSQL.Extensions;

public sealed class PostgreSqlStorageOptions
{
    public int MaximumPoolSize { get; set; } = 32;

    public int MaximumConcurrentOperations { get; set; } = 32;

    public void Validate()
    {
        if (MaximumPoolSize is < 1 or > 10_000)
        {
            throw new InvalidOperationException(
                "PostgreSQL MaximumPoolSize must be between 1 and 10000.");
        }

        if (MaximumConcurrentOperations is < 1 or > 10_000)
        {
            throw new InvalidOperationException(
                "PostgreSQL MaximumConcurrentOperations must be between 1 and 10000.");
        }
    }
}
