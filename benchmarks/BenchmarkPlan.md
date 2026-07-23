# BenchmarkDotNet / Load Test Plan

- JobRegistry discovery：启动耗时、分配、100/1000 handlers。
- Worker idle 10 分钟：allocated B/op、Gen0、HTTP 请求数。
- Payload decode：1 KiB/64 KiB/1 MiB，typed 与 raw IKubeJobV2。
- claim SQL：10K/1M Pending，1/4/16 并发 Server。
- complete/retry SQL：成功、失败重排、取消。
- 100/1000 Worker LISTEN/NOTIFY 唤醒延迟。
- 16/256/4096 shard materialization。

发布门槛应基于与当前 main 的对比数据，而不是绝对宣传数字。基准机、PostgreSQL 参数、数据量和 GC 模式必须固化。
