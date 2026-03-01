using System;

namespace KubeJob.Core.Enums
{
    /// <summary>
    /// Represents the current execution state of a Job Run.
    /// </summary>
    public enum JobStatus
    {
        /// <summary>
        /// Job is created and waiting to be assigned to a worker.
        /// </summary>
        Pending = 0,

        /// <summary>
        /// Job has been assigned to a specific worker but hasn't started yet.
        /// </summary>
        Assigned = 1,

        /// <summary>
        /// Job is currently executing on the worker node.
        /// </summary>
        Running = 2,

        /// <summary>
        /// Job has completed execution successfully.
        /// </summary>
        Succeeded = 3,

        /// <summary>
        /// Job execution threw an exception or timed out.
        /// </summary>
        Failed = 4,

        /// <summary>
        /// Job was intentionally canceled before or during execution.
        /// </summary>
        Canceled = 5
    }
}