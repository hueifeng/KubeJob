# Distributed Runtime Test Matrix

## PostgreSQL Testcontainers 集成测试

1. 两个 Server 同时 claim 1000 Run：每个 `(RunId,Attempt)` 只产生一个 LeaseToken。
2. Server 在 UPDATE 前、提交前、提交后断开。
3. Worker A lease 过期，Worker B claim；A 的 renew/complete 全部被拒绝。
4. 同 WorkerId 新 Session 注册后，旧 Session 所有写入被拒绝。
5. duplicate complete：已成功响应丢失后的重试保持幂等。
6. fail + retry 与 cancel 竞争。
7. Pending、Running 取消；并覆盖旧版 Assigned 兼容状态。
8. MaxRetries=0/1/3 的总 attempt 数。
9. DB 权威容量阻止并发 claim 超过 MaxCapacity。
10. queue filter、priority、AvailableAt、NodeSelector、handler/schema version。
11. Cron 多 Server 同时 materialize，无重复 fire；cursor 和 runs 原子提交。
12. Broadcast 固定 session 快照，扩缩容不改变已有 batch。
13. LISTEN 通知发生在第一次空 claim 与 wait 之间，不丢唤醒。
14. LISTEN 断线时，长轮询超时仍能取到任务。
15. 4096 shards、1 MiB Payload 的内存上限。
16. idempotency 相同/不同 payload hash。
17. 修改 JobSpec 后，已排队 Run 的执行快照不变化。
18. History cleanup 后 Payload/Attempt/Submission 孤儿被回收。

## Chaos

- kill -9 Worker
- 网络单向隔离
- 30 秒进程暂停后恢复
- PostgreSQL failover/restart
- Server 滚动升级
- Worker 新旧 HandlerVersion 混跑
