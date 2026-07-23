# 安全与运维发布门槛

当前基础代码没有实现生产认证。公开网络部署前必须补齐：

- Worker 双向 TLS 或短期签名凭据。
- WorkerId/SessionId 不作为身份凭据；它们只是 fencing 字段。
- Server API 按注册、claim、renew、complete 分权。
- Payload 静态加密、传输加密、日志脱敏。
- Dashboard 与控制 API 独立认证授权。
- 请求体、Payload、标签、能力数、批量数限额。
- 每 Worker/租户 rate limit 和审计日志。
- PostgreSQL 最小权限账户；迁移账户与运行账户分离。

运维指标至少包括 Pending/Running（以及旧版 Assigned）、过期租约、重试、claim 延迟、版本不匹配、通知重连、DB 连接池耗尽。
