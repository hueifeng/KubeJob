using System;
using Microsoft.Extensions.DependencyInjection;
using KubeJob.Server.Data;

namespace KubeJob.Server.Options
{
    /// <summary>
    /// Configuration options for the KubeJob Server (Control Plane).
    /// </summary>
    public class KubeJobServerOptions
    {
        /// <summary>
        /// Indicates if the server is using purely in-memory storage.
        /// </summary>
        public bool UseInMemoryStorage { get; set; } = false;

        /// <summary>
        /// Enables the legacy cron, dispatcher, node-health, and history services.
        /// Kept enabled by default for backwards compatibility during migration.
        /// </summary>
        public bool EnableLegacyHostedServices { get; set; } = true;

        /// <summary>
        /// Enables the V2 schedule, lease-reaper, and outbox services.
        /// </summary>
        public bool EnableV2HostedServices { get; set; } = true;

        /// <summary>
        /// A delegate to configure the storage backend (e.g., PostgreSQL).
        /// </summary>
        public Action<IServiceCollection>? StorageConfigurator { get; set; }

        /// <summary>
        /// A delegate to configure the distributed lock provider (e.g., PostgreSQL, Redis).
        /// </summary>
        public Action<IServiceCollection>? LockConfigurator { get; set; }

        /// <summary>
        /// Runs only the V2 control-plane background services.
        /// </summary>
        public KubeJobServerOptions UseV2Only()
        {
            EnableLegacyHostedServices = false;
            EnableV2HostedServices = true;
            return this;
        }

        /// <summary>
        /// Runs only the legacy control-plane background services.
        /// </summary>
        public KubeJobServerOptions UseLegacyOnly()
        {
            EnableLegacyHostedServices = true;
            EnableV2HostedServices = false;
            return this;
        }

        /// <summary>
        /// Runs legacy and V2 background services side by side for migration.
        /// The runtimes use separate durable schemas.
        /// </summary>
        public KubeJobServerOptions UseDualRuntime()
        {
            EnableLegacyHostedServices = true;
            EnableV2HostedServices = true;
            return this;
        }

        /// <summary>
        /// Configures the KubeJob Server to use In-Memory storage and locking.
        /// Suitable for development or single-node deployments.
        /// </summary>
        /// <returns>The updated options.</returns>
        public KubeJobServerOptions UseInMemory()
        {
            UseInMemoryStorage = true;
            StorageConfigurator = services =>
            {
                services.AddSingleton<KubeJob.Server.Data.IKubeJobRepository, KubeJob.Server.Data.InMemoryKubeJobRepository>();
            };

            if (LockConfigurator == null)
            {
                LockConfigurator = services =>
                {
                    services.AddSingleton<IKubeJobLockProvider, InMemoryLockProvider>();
                };
            }

            return this;
        }
    }
}
