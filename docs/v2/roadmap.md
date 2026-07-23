# Post-Core V2 Roadmap

The current V2 core establishes typed submission, logical Run/Attempt state,
pull scheduling, leases/fencing, Worker Sessions, independent schedules,
transactional Outbox delivery, in-process/HTTP worker transports, and optional
RabbitMQ notification acceleration.

The following features should remain separate follow-up changes so they do not
weaken the reviewability or correctness of the core state machine:

1. **V2 Dashboard and operational query service**
   - Queue depth, oldest ready age, throughput, active workers, attempts, and
     conditions.
   - Sanitized DTOs only; never expose lease or fencing credentials.
   - Dashboard depends on query/admin services, not storage repositories.

2. **Versioned capabilities and placement**
   - Handler version and payload schema compatibility.
   - Worker label selectors, preferred regions, and capacity classes.
   - Rolling upgrade and build pinning tests.

3. **Durable batches, sharding, and broadcast**
   - Explicit JobBatch aggregate.
   - Bounded batch creation and MaxParallelism.
   - Broadcast target Session snapshot and offline policy.

4. **Retention, archive, and performance gates**
   - Active/history separation.
   - Partial-index query plans with `EXPLAIN (ANALYZE, BUFFERS)`.
   - 100k-job load tests and BenchmarkDotNet baselines.

5. **Optional workflows**
   - Separate `KubeJob.Workflows` package.
   - Definition/Run separation, DAG dependencies, outputs, memoization, and
     waiting without occupying a worker slot.

These items build on the V2 contracts rather than changing handler code or the
core Run/Attempt/lease semantics.
