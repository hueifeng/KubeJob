using KubeJob.ControlPlane.Data;
using KubeJob.ControlPlane.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Runtime;
using KubeJob.Storage.PostgreSQL.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Extensions;

public static class KubeJobPostgresExtensions
{
    public static IServiceCollection AddKubeJobPostgreSql(
        this IServiceCollection services,
        string connectionString,
        Action<PostgreSqlStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException(
                "A PostgreSQL connection string is required.",
                nameof(connectionString));
        }

        var storageOptions = new PostgreSqlStorageOptions();
        configure?.Invoke(storageOptions);
        storageOptions.Validate();

        var businessConnectionOptions = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = storageOptions.BusinessPoolSize
        };
        var backgroundConnectionOptions = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = storageOptions.BackgroundPoolSize
        };

        services.AddMetrics();
        services.AddSingleton(storageOptions);
        services.AddSingleton<KubeJobPostgreSqlMetrics>();
        services.AddSingleton<IStorageInitializer>(_ => new DbInitializer(businessConnectionOptions.ConnectionString));
        services.AddKeyedSingleton(
            PostgreSqlDataSourceKind.Business,
            (_, _) => NpgsqlDataSource.Create(businessConnectionOptions.ConnectionString));
        services.AddKeyedSingleton(
            PostgreSqlDataSourceKind.Background,
            (_, _) => NpgsqlDataSource.Create(backgroundConnectionOptions.ConnectionString));
        services.AddSingleton(sp =>
        {
            var runtimeOptions = sp.GetRequiredService<IOptions<JobRuntimeOptions>>().Value;
            storageOptions.ValidateCapacity(runtimeOptions.OutboxPublishConcurrency);
            return new PostgreSqlJobRuntimeStore(
                sp.GetRequiredKeyedService<NpgsqlDataSource>(PostgreSqlDataSourceKind.Business),
                sp.GetRequiredKeyedService<NpgsqlDataSource>(PostgreSqlDataSourceKind.Background),
                storageOptions,
                sp.GetService<KubeJobPostgreSqlMetrics>());
        });
        services.AddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobScheduleStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobRuntimeDashboardStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        services.AddSingleton<IJobRuntimeMaintenanceStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        return services;
    }
}

internal enum PostgreSqlDataSourceKind
{
    Business,
    Background
}