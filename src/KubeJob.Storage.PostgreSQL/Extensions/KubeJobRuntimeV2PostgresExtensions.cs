using KubeJob.Server.Data;
using KubeJob.Server.Options;
using KubeJob.Storage.PostgreSQL.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace KubeJob.Storage.PostgreSQL.Extensions;

public static class KubeJobRuntimeV2PostgresExtensions
{
    public static KubeJobServerOptions UsePostgreSqlRuntimeV2(this KubeJobServerOptions options,
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        options.UsePostgreSql(connectionString);
        options.RuntimeMode=KubeJobRuntimeMode.LeaseV2;
        options.RuntimeConfigurator=services=>AddRuntime(services,connectionString);
        return options;
    }

    public static IServiceCollection AddKubeJobRuntimeV2PostgreSql(this IServiceCollection services,
        string connectionString)
    {
        AddRuntime(services,connectionString);
        return services;
    }

    private static void AddRuntime(IServiceCollection services,string connectionString)
    {
        services.AddSingleton(_=>NpgsqlDataSource.Create(connectionString));
        services.AddSingleton<PostgreSqlJobAvailabilitySignal>();
        services.AddSingleton<IJobAvailabilitySignal>(sp=>sp.GetRequiredService<PostgreSqlJobAvailabilitySignal>());
        services.AddSingleton<IHostedService>(sp=>sp.GetRequiredService<PostgreSqlJobAvailabilitySignal>());
        services.AddSingleton<IKubeJobRuntimeRepository,PostgreSqlRuntimeRepository>();
        services.AddSingleton<IKubeJobScheduleMaterializer,PostgreSqlScheduleMaterializer>();
        services.AddSingleton<IKubeJobSubmissionRepository,PostgreSqlJobSubmissionRepository>();
    }
}
