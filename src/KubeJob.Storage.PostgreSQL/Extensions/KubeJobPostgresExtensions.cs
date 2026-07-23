using System;
using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Server.Runtime;
using KubeJob.Storage.PostgreSQL.Data;
using KubeJob.Storage.PostgreSQL.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Extensions
{
    public static class KubeJobPostgresExtensions
    {
        public static KubeJobServerOptions UsePostgreSql(this KubeJobServerOptions options, string connectionString = "")
        {
            ArgumentNullException.ThrowIfNull(options);
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("A PostgreSQL connection string is required.", nameof(connectionString));
            }

            options.StorageConfigurator = services =>
            {
                services.AddSingleton<IKubeJobRepository>(_ => new KubeJobRepository(connectionString));
                services.AddSingleton<KubeJob.Server.Data.IStorageInitializer>(_ => new DbInitializer(connectionString));

                services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
                services.AddSingleton<PostgreSqlJobRuntimeStore>();
                services.AddSingleton<IJobSubmissionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IWorkerSessionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IJobClaimStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IJobCompletionStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IJobQueryStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IJobScheduleStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
                services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<PostgreSqlJobRuntimeStore>());
            };

            if (options.LockConfigurator == null)
            {
                options.LockConfigurator = services =>
                {
                    services.AddSingleton<IKubeJobLockProvider>(_ => new PostgreSqlLockProvider(connectionString));
                };
            }

            return options;
        }
    }
}
