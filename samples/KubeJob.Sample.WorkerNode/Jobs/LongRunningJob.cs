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
    [KubeJob("long-running-job", Cron = "0 0 * * *", ExecuteModel = ExecuteModel.Standalone)]
    public class LongRunningJob : IKubeJob
    {
        public async Task ExecuteAsync(KubeJobContext context, CancellationToken token)
        {
            context.Logger.LogInformation("LongRunningJob started. Will run for 60 seconds. You can test graceful shutdown now.");
            
            for (int i = 0; i < 60; i++)
            {
                token.ThrowIfCancellationRequested();
                context.Logger.LogInformation("LongRunningJob step {Step}/60...", i + 1);
                await Task.Delay(1000, token);
            }

            context.Logger.LogInformation("LongRunningJob completed successfully.");
        }
    }
}
