# V2 Security and Trust Boundaries

KubeJob V2 separates user-facing query models from worker protocol credentials.

## Never expose

The following values are internal worker-protocol credentials and must not be
returned by Dashboard or public job-query APIs:

```text
LeaseToken
FencingToken
raw transport acknowledgement identifiers
storage connection information
```

Attempt history uses `JobAttemptSnapshot`, which omits those values.

## Internal control-plane endpoints

Worker registration, claim, renewal, and completion endpoints are intended for
an authenticated internal network or service identity. Deployment hosts should
apply their normal ASP.NET Core authentication and authorization policy before
exposing these routes outside a trusted network.

A LeaseToken is a fencing component, not an authentication mechanism.

## Payloads and failures

Payload JSON and exception messages can contain sensitive data. Operators should
apply retention, redaction, and access policies suitable for their application.
Handler code should avoid placing secrets in exception text.

## RabbitMQ

RabbitMQ messages contain only queue and Run wake-up metadata. They do not carry
handler payloads or grant execution ownership. Use TLS and broker credentials in
production even though message loss or duplication cannot violate job-state
correctness.
