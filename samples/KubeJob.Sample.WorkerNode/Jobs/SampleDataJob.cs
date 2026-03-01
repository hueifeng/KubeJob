using System;
using System.Threading;
using System.Threading.Tasks;
using KubeJob.Core.Attributes;
using KubeJob.Core.Context;
using KubeJob.Core.Enums;
using KubeJob.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace KubeJob.Sample.WorkerNode.Jobs
{
    [KubeJob("sample-job-1", Cron = "*/1 * * * *", ExecuteModel = ExecuteModel.Standalone)]
    [NodeSelector("env", "dev")]
    public class SampleDataJob : IKubeJob
    {
        public async Task ExecuteAsync(KubeJobContext context, CancellationToken token)
        {
            context.Logger.LogInformation("Starting SampleDataJob on Shard {ShardIndex}/{TotalShards}", context.ShardIndex, context.TotalShards);
            
            // Simulate work
            for (int i = 0; i < 5; i++)
            {
                token.ThrowIfCancellationRequested();
                context.Logger.LogInformation("Working... Step {Step}", i + 1);
                await Task.Delay(1000, token);
            }

            context.Logger.LogInformation("SampleDataJob Completed.");
        }
    }
}
