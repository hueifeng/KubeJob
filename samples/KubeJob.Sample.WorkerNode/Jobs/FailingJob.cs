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
    [KubeJob("failing-job", Cron = "*/2 * * * *", MaxRetries = 2, ExecuteModel = ExecuteModel.Standalone)]
    [NodeSelector("env", "dev")]
    public class FailingJob : IKubeJob
    {
        public async Task ExecuteAsync(KubeJobContext context, CancellationToken token)
        {
            context.Logger.LogInformation("Starting FailingJob. This job will throw an exception.");
            await Task.Delay(1000, token);
            throw new Exception("Intentional failure to test MaxRetries");
        }
    }
}
