# V2 Core Completion Criteria

The V2 core is considered implementation-complete when the Draft PR satisfies
all of the following:

- Typed handler and generated JobKey compile.
- Legacy non-generic handler still compiles.
- Unified hosting resolves the in-process worker transport.
- Remote worker resolves the HTTP worker transport.
- Concurrent workers cannot claim the same current Attempt.
- A replaced Worker Session cannot renew or complete current state.
- Lease expiry requeues or terminates according to attempt policy.
- Idempotency conflicts are explicit and race-safe.
- Schedule fire advances the cursor and inserts Run + Outbox atomically.
- Outbox publishing claims recover after publisher crashes.
- Attempt query DTOs do not expose lease or fencing credentials.
- RabbitMQ notifications remain non-authoritative and polling remains a fallback.
- Unit tests, real PostgreSQL integration tests, and package validation pass.
- NuGet output includes the typed-key analyzer and all declared V2 packages.

Advanced Dashboard UI, placement/versioning, batches, broadcast, sharding,
archive, performance gates, and workflows are post-core extensions and must not
change the accepted Run/Attempt/lease semantics.
