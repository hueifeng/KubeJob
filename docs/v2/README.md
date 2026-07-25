# KubeJob Documentation

- [Local development stack](./local-development.md)
- [本地开发环境](./local-development.zh-CN.md)
- [Getting Started](./getting-started.md)
- [中文使用指南](./getting-started.zh-CN.md)
- [Runtime Architecture](./architecture.md)
- [Security and Trust Boundaries](./security.md)
- [V2 Hardening Review](./hardening-review.md)
- [RabbitMQ Notification Acceleration](./rabbitmq-notifications.md)
- [Completion Criteria](./completion-criteria.md)
- [Post-Runtime Roadmap](./roadmap.md)
- [Architecture Decision Records](../adr/)

The current runtime is V2-only. Typed handlers, logical Runs, physical Attempts,
Worker Sessions, independent Schedules, the transactional Outbox, and the
operator Dashboard use the `Kj2_*` schema and V2 contracts. The previous legacy
runtime is not retained as a compatibility mode.
