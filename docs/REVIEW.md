# 对当前 KubeJob 的结论

当前 main 的优点是概念小、代码容易读、Standalone/Sharding/Broadcast 入口已经形成。但它更接近“分布式 Job 原型”，还不是可放心承载关键生产任务的调度器。

主要原因：

- Worker 状态上报没有 attempt/lease fencing。
- 节点离线被当作任务所有权依据，网络抖动会造成重复和旧结果覆盖。
- Dispatcher 是单点热路径，并周期性全量读取/解析。
- Worker 反射扫描发生在执行路径，返回多少任务就启动多少 Task。
- WorkerId 不能区分同一节点上的旧进程与新进程。
- Job 注册会覆盖配置，滚动发布缺少 handler/payload 版本兼容。
- Cron leader lock 和实际写入不是同一事务。
- 缺少 typed enqueue、幂等、延迟执行、批次取消、版本错误解释等易用能力。

V2 基础包针对这些问题重新定义执行语义。它仍然不是“下载后即 production-ready”：认证、真实 PostgreSQL 集成测试、chaos、基准和迁移演练必须完成。
