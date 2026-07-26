using KubeJob.Core.Runtime;

namespace KubeJob.Server.ControlPlane;

/// <summary>
/// Converts an external broker message into one durable KubeJob submission.
/// The source and broker message ID form the idempotency identity so broker
/// redelivery cannot create a second logical Run.
/// </summary>
public sealed class JobMessageIngress : IJobMessageIngress
{
    private readonly JobControlPlane _controlPlane;

    public JobMessageIngress(JobControlPlane controlPlane)
    {
        _controlPlane = controlPlane;
    }

    public async ValueTask<JobIngressResult> SubmitAsync(
        JobIngressMessage message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (string.IsNullOrWhiteSpace(message.Source)
            || string.IsNullOrWhiteSpace(message.MessageId))
        {
            throw new ControlPlaneValidationException(
                "invalid_ingress_identity",
                "Ingress Source and MessageId are required and must be stable across broker redelivery.");
        }

        var request = message.Job with
        {
            IdempotencyKey = $"{message.Source.Trim()}:{message.MessageId.Trim()}"
        };
        var receipt = await _controlPlane.SubmitAsync(request, cancellationToken);
        return new JobIngressResult(receipt.Handle.JobId, receipt.Existing);
    }
}
