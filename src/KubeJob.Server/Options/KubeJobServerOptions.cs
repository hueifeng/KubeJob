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
        /// <summary>Execution runtime. LegacyDispatcher remains the compatibility default.</summary>
        public KubeJobRuntimeMode RuntimeMode { get; set; } = KubeJobRuntimeMode.LegacyDispatcher;

        /// <summary>Registers the V2 runtime storage and signalling services.</summary>
        public Action<IServiceCollection>? RuntimeConfigurator { get; set; }

        /// <summary>V2 client limits. All payloads and control-plane responses remain bounded.</summary>
        public KubeJobClientOptions ClientOptions { get; } = new();

        /// <summary>Opt-in authentication hook. V2 is not production-ready until configured.</summary>
        public Action<Microsoft.AspNetCore.Authentication.AuthenticationBuilder>? AuthenticationConfigurator { get; set; }

        /// <summary>
        /// Indicates if the server is using purely in-memory storage.
        /// </summary>
        public bool UseInMemoryStorage { get; set; } = false;
        
        /// <summary>
        /// A delegate to configure the storage backend (e.g., PostgreSQL).
        /// </summary>
        public Action<IServiceCollection>? StorageConfigurator { get; set; }
        
        /// <summary>
        /// A delegate to configure the distributed lock provider (e.g., PostgreSQL, Redis).
        /// </summary>
        public Action<IServiceCollection>? LockConfigurator { get; set; }

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

        public KubeJobServerOptions UseLeaseV2(Action<IServiceCollection> runtimeConfigurator)
        {
            ArgumentNullException.ThrowIfNull(runtimeConfigurator);
            RuntimeMode = KubeJobRuntimeMode.LeaseV2;
            RuntimeConfigurator = runtimeConfigurator;
            return this;
        }
    }
}
