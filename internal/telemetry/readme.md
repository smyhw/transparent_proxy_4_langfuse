# internal/telemetry

遥测层:把解析结果构建为 OTel span 并上报 Langfuse(原生 OTel 接入)。

关键设计:
- `telemetry.go`:OTLP HTTP exporter(protobuf)直达 `{endpoint}/api/public/otel/v1/traces`,
  携带 Basic Auth(公钥:密钥)与 `x-langfuse-ingestion-version` 头;批量处理器参数可配置;
- `span.go`:一次 LLM 调用 = 一条 trace = 一个 generation span,时间戳取真实起止时刻;
  属性映射遵循 Langfuse 官方文档(`gen_ai.prompt`→input、`gen_ai.completion`→output、
  `gen_ai.usage.*`→usage、`langfuse.observation.type=generation` 等);`langfuse.user.id` 来源优先级:客户端 API key(需开启 `user_key_as_user_id`)→ 配置请求头 → 请求体 user 字段;
- `worker.go`:独立协程池消费队列,与转发路径完全解耦;解析失败降级为只计数不上报。
