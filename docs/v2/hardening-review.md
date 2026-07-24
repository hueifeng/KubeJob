# V2 Hardening Review

This review was performed after the V2-only runtime and Dashboard landed.

## Removed or corrected

- replaced the obsolete Chinese V1 README with links to the canonical V2 guides;
- renamed the remote sample from `WorkerNode` to `RemoteWorker`;
- removed the unused template HTTP request that targeted a nonexistent endpoint;
- reduced the solution to the supported Any CPU Debug/Release configurations.

## Runtime hardening

- a fenced Worker Session stops claiming, cancels local Attempts, and fails the hosted service so a process supervisor can restart it with a new SessionId;
- Worker options normalize queues and labels and reject ambiguous or unbounded metadata;
- persisted failure details are bounded while complete exceptions remain available in application logs.

## Dashboard hardening

- list queries use a payload-free Run projection;
- Payload JSON is fetched only for an explicitly opened Run detail page;
- Worker Session and Schedule views have configurable hard limits;
- PostgreSQL includes indexes for Dashboard filtering and ordering;
- the embedded UI is self-contained and has no public CDN dependency;
- mutating actions report missing, terminal, or concurrently changed resources instead of silently redirecting.

## Compatibility

No V1 runtime compatibility path was restored. The non-generic Handler, Push Dispatcher,
JobSpec, WorkerNode model, legacy tables, repositories, locks, and legacy Dashboard remain removed.
