using System;
using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Storage.PostgreSQL.Data;
using Microsoft.Extensions.DependencyInjection;

namespace KubeJob.Storage.PostgreSQL.Extensions
{
    public static class KubeJobPostgresExtensions
    {
        public static KubeJobServerOptions UsePostgreSql(this KubeJobServerOptions options, string connectionString = "")
        {
            options.StorageConfigurator = services =>
            {
                services.AddSingleton<IKubeJobRepository>(sp => new KubeJobRepository(connectionString));
                services.AddSingleton<KubeJob.Server.Data.IStorageInitializer>(sp => new DbInitializer(connectionString));
            };
            
            if (options.LockConfigurator == null)
            {
                options.LockConfigurator = services =>
                {
                    services.AddSingleton<IKubeJobLockProvider>(sp => new PostgreSqlLockProvider(connectionString));
                };
            }

            return options;
        }
    }
}
