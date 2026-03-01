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
    [KubeJob("broadcast-test-job", Cron = "0 0 * * *", ExecuteModel = ExecuteModel.Broadcast)]
    public class BroadcastJob : IKubeJob
    {
        public async Task ExecuteAsync(KubeJobContext context, CancellationToken token)
        {
            context.Logger.LogInformation("BroadcastJob running. Every active node gets to run this!");
            await Task.Delay(2000, token);
            context.Logger.LogInformation("BroadcastJob completed.");
        }
    }
}
