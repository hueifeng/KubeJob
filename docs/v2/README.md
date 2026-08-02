# KubeJob Documentation

- [Local development stack](./local-development.md)
- [本地开发环境](./local-development.zh-CN.md)
- [Getting Started](./getting-started.md)
- [中文使用指南](./getting-started.zh-CN.md)
- [Runtime Architecture](./architecture.md)
- [Logical Architecture and Sequences](./logical-architecture.md)
- [目标架构（中文）](./target-architecture.zh-CN.md)
- [任务提交与物理投递解耦](../adr/011-hide-physical-delivery-from-job-submitters.md)
- [Control-plane adapter decision](../adr/009-converge-transports-on-control-plane-modules.md)
- [Security and Trust Boundaries](./security.md)
- [Telemetry and host-side OpenTelemetry setup](./telemetry.md)
- [V2 Hardening Review](./hardening-review.md)
- [RabbitMQ Notification Acceleration](./rabbitmq-notifications.md)
- [Message Transport Adapters](./message-transport.md)
- [消息顺序与吞吐：成熟中间件的共同设计](./message-ordering-research.zh-CN.md)
- [Completion Criteria](./completion-criteria.md)
- [Post-Runtime Roadmap](./roadmap.md)
- [Architecture Decision Records](../adr/)

The current runtime is V2-only. Typed handlers, logical Runs, physical Attempts,
Worker Sessions, independent Schedules, the transactional Outbox, and the
operator Dashboard use the `Kj2_*` schema and V2 contracts. The previous legacy
runtime is not retained as a compatibility mode.
