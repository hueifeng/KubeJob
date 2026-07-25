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
        /// A delegate to configure the storage backend (e.g., PostgreSQL).
        /// </summary>
        public Action<IServiceCollection>? StorageConfigurator { get; set; }
        
        /// <summary>
        /// A delegate to configure the distributed lock provider (e.g., PostgreSQL, Redis).
        /// </summary>
        public Action<IServiceCollection>? LockConfigurator { get; set; }

        /// <summary>
        /// Enables the seed endpoint used for demo data injection.
        /// Should remain disabled outside local demos.
        /// </summary>
        public bool EnableSeedEndpoint { get; set; } = false;

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
