# Post-Runtime Roadmap

> Historical V2 roadmap. It is retained for design context and is not a list of
> current V3 commitments.

The V2 runtime included typed submission, logical Run/Attempt state, pull
scheduling, leases/fencing, Worker Sessions, independent schedules,
transactional Outbox delivery, in-process/HTTP worker transports, optional
RabbitMQ notification acceleration, and a V2-native operator Dashboard.

The following features should remain separate follow-up changes so they do not
weaken the reviewability or correctness of the state machine:

1. **Versioned capabilities and placement**
   - Handler version and payload schema compatibility.
   - Worker label selectors, preferred regions, and capacity classes.
   - Rolling upgrade and build pinning tests.

2. **Durable batches, sharding, and broadcast**
   - Explicit JobBatch aggregate.
   - Bounded batch creation and MaxParallelism.
   - Broadcast target Session snapshot and offline policy.

3. **Retention, archive, and performance gates**
   - Retention of published outbox rows and unkeyed terminal runs is implemented
     (`RuntimeRetentionService`); active/history separation and archive are not.
   - Partial-index query plans with `EXPLAIN (ANALYZE, BUFFERS)`.
   - 100k-job load tests (the benchmark harness in `tests/KubeJob.Benchmark/`
     tops out around 5k jobs per run) and BenchmarkDotNet baselines.
   - Dashboard oldest-ready-age is implemented; throughput views remain.

4. **Optional workflows**
   - Separate `KubeJob.Workflows` package.
   - Definition/Run separation, DAG dependencies, outputs, memoization, and
     waiting without occupying a worker slot.

These items build on the current contracts rather than changing handler code or
the core Run/Attempt/lease semantics.
