# Security and Trust Boundaries

KubeJob separates user-facing query models, operator pages, and internal worker
protocol credentials.

## Never expose

The following values are internal coordination credentials and must not be
returned by the Dashboard or public job-query APIs:

```text
LeaseToken
FencingToken
raw transport acknowledgement identifiers
storage connection information
```

Attempt APIs use `JobAttemptSnapshot`, which omits those values. The Dashboard
renders safe runtime records and never renders lease or fencing credentials.

## Dashboard

The embedded Dashboard is an operator surface, not a public application page.
The host should bind it to an ASP.NET Core authorization policy:

```csharp
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("KubeJobDashboard", policy =>
        policy.RequireRole("KubeJobOperator"));
});

builder.Services.AddKubeJobDashboard(options =>
{
    options.RoutePrefix = "admin/jobs";
    options.AuthorizationPolicy = "KubeJobDashboard";
    options.ShowPayloads = false;
    options.AllowMutatingActions = false;
});
```

The host must add its authentication scheme and call `UseAuthentication()` and
`UseAuthorization()` before mapping controllers.

Safe defaults:

- the Dashboard is read-only;
- payload JSON is hidden;
- Run cancellation is unavailable;
- Schedule enable/disable is unavailable.

Set `ShowPayloads` only when payload disclosure is acceptable. Set
`AllowMutatingActions` only for operators who are authorized to change runtime
state. Anti-forgery validation remains enabled on Dashboard POST actions.

## Internal control-plane endpoints

Worker registration, claim, renewal, heartbeat, and completion endpoints are
intended for an authenticated internal network or workload identity. Deployment
hosts should apply their normal ASP.NET Core authentication and authorization
policy before exposing those routes outside a trusted network.

A LeaseToken is a fencing component, not an authentication mechanism.

## Payloads and failures

Payload JSON and exception messages can contain sensitive data. Apply retention,
redaction, and access policies suitable for the application. Handler code should
avoid placing secrets in exception text.

## RabbitMQ

RabbitMQ messages contain queue and Run wake-up metadata. They do not carry
handler payloads or grant execution ownership. Use TLS and broker credentials in
production even though message loss or duplication cannot violate job-state
correctness.
