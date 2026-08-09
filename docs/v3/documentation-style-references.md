# Documentation notes for maintainers

This page records the small set of writing habits we borrowed from the
official .NET, Hangfire, Quartz.NET, and MassTransit guides. It is a reference
for contributors, not a user tutorial.

## A useful page order

1. Say who the page is for and what the reader will accomplish.
2. List prerequisites, versions, services, credentials, and local-only limits.
3. Show the smallest runnable example.
4. Explain what is stored, who owns retries and acknowledgement, and what can
   happen twice.
5. Finish with verification, logs/health checks, and safe cleanup.

This sequence follows the practical shape of the [Microsoft Worker Services
guide](https://learn.microsoft.com/en-us/dotnet/core/extensions/workers),
[Hangfire Getting Started](https://docs.hangfire.io/en/latest/getting-started/),
[Quartz Quick Start](https://www.quartz-scheduler.net/documentation/quartz-3.x/quick-start),
and [MassTransit configuration](https://masstransit.io/documentation/configuration).

## Wording to prefer

Use concrete verbs: “stores the attempt”, “claims a lease”, “redelivers the
message”, “acknowledges after the handler succeeds”. Avoid broad promises such
as “reliable” or “exactly once” unless the page defines the failure window.
Every KubeJob page should make the following visible:

- PostgresManaged stores job state and leases in PostgreSQL.
- BrokerNative leaves delivery and acknowledgement to the broker.
- Both paths are at-least-once; external side effects need duplicate-safe
  handlers.
- Local credentials, reset commands, and anonymous access are not production
  settings.

## Before merging a doc change

Run the commands in [Local development](local-development.md), check every
relative link, and make sure examples use public extension methods that still
exist in the source tree. If a claim comes from another project, link its
first-party guide and avoid copying its wording. If a default depends on a
library version, name the version instead of presenting it as a KubeJob rule.
