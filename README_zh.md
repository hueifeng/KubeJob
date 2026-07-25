# KubeJob 中文文档

KubeJob 当前是 **V2-only** 的强类型 .NET 分布式后台任务运行时。

请使用以下最新文档：

- [中文使用指南](./docs/v2/getting-started.zh-CN.md)
- [V2 架构说明](./docs/v2/architecture.md)
- [Dashboard 与安全边界](./docs/v2/security.md)
- [英文首页](./README.md)

当前运行时使用 `IKubeJob<TPayload>`、逻辑 Run、物理 Attempt、Pull Worker、
Worker Session fencing、PostgreSQL 事务、Outbox 和独立 Schedule。

旧的非泛型 Handler、Push Dispatcher、JobSpec、WorkerNode、旧数据库表和旧 Dashboard
已经删除，不提供兼容模式。
