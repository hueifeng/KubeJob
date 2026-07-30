using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Runtime;
using KubeJob.Storage.PostgreSQL.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Extensions;

public static class KubeJobPostgresExtensions
{
    public static KubeJobServerOptions UsePostgreSql(
        this KubeJobServerOptions options,
        string connectionString)
        => options.UsePostgreSql(connectionString, configure: null);

    public static KubeJobServerOptions UsePostgreSql(
        this KubeJobServerOptions options,
        string connectionString,
        Action<PostgreSqlStorageOptions>? configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException(
                "A PostgreSQL connection string is required.",
                nameof(connectionString));
        }

        var storageOptions = new PostgreSqlStorageOptions();
        configure?.Invoke(storageOptions);
        storageOptions.Validate();

        var connectionOptions = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = storageOptions.MaximumPoolSize
        };

        options.StorageConfigurator = services =>
        {
            services.AddMetrics();
            services.AddSingleton(storageOptions);
            services.AddSingleton<KubeJobPostgreSqlMetrics>();
            services.AddSingleton<IStorageInitializer>(_ => new DbInitializer(connectionOptions.ConnectionString));
            services.AddSingleton(_ => NpgsqlDataSource.Create(connectionOptions.ConnectionString));
            services.AddSingleton<PostgreSqlJobRuntimeStore>();
            services.AddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobScheduleStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobRuntimeDashboardStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            services.AddSingleton<IJobRuntimeMaintenanceStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
        };

        return options;
    }
}
