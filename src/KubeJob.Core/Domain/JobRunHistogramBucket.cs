using System;
using KubeJob.Core.Enums;

namespace KubeJob.Core.Domain
{
    /// <summary>
    /// An aggregated count of job runs grouped by time bucket and status.
    /// Used by the dashboard to render activity graphs without loading raw rows.
    /// </summary>
    public class JobRunHistogramBucket
    {
        /// <summary>
        /// Start of the time bucket, in UTC.
        /// </summary>
        public DateTime BucketUtc { get; set; }

        public JobStatus Status { get; set; }

        public int Count { get; set; }
    }
}
