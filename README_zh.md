# KubeJob 中文入口

KubeJob 是一个 .NET 后台任务库。需要把工作从 HTTP 请求中剥离出来时，
可以使用 PostgreSQL 保存任务状态、重试和租约；如果系统已经以消息
Broker 为中心，也可以直接消费 BrokerNative 任务和事件。

先看英文首页的 [Quick start](README.md#quick-start)，里面包含完整的安装、
代码示例和安全配置说明。

- [运行方式怎么选](docs/v3/runtime-model.md)
- [本地开发（含 Podman）](docs/v3/local-development.md)
- [事件订阅](docs/v3/events.md)
- [传输适配器](docs/v3/transport.md)
- [基准测试](docs/v3/benchmarking.md)
- [发布检查清单](docs/v3/release-checklist.md)

代码和当前文档都以英文版本为准；如果中文说明与代码不一致，请以英文
文档和实际 API 为准。

## License

[MIT License](LICENSE)
