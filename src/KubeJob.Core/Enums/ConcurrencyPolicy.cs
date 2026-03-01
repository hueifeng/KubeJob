namespace KubeJob.Core.Enums
{
    public enum ConcurrencyPolicy
    {
        /// <summary>
        /// Allows concurrently running jobs.
        /// </summary>
        Allow = 0,

        /// <summary>
        /// Skips next run if previous hasn't finished.
        /// </summary>
        Forbid = 1,

        /// <summary>
        /// Cancels currently running job and replaces it.
        /// </summary>
        Replace = 2
    }
}
