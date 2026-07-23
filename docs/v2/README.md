# KubeJob V2 Documentation

- [Getting Started](./getting-started.md)
- [中文使用指南](./getting-started.zh-CN.md)
- [Runtime Architecture](./architecture.md)
- [RabbitMQ Notification Acceleration](./rabbitmq-notifications.md)
- [Architecture Decision Records](../adr/)

V2 is additive. Legacy handlers and tables remain available during the migration
window; new typed handlers, Runs, Attempts, Worker Sessions, Schedules, and the
Outbox use the V2 APIs and `Kj2_*` schema.
