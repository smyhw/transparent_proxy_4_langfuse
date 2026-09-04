# internal/parser

解析层:把捕获的 HTTP 流量解析为归一化的 LLM 调用结果(`Result`),供遥测层构建 span。

关键设计:
- `Parser` 接口二分:`Match`(header 级,极快)与 `Parse`(body 级,仅 worker 调用);
- 三个解析器:OpenAI Chat Completions、Anthropic Messages、OpenAI Responses,均兼容 JSON 与 SSE 流式;
- `sseDecoder` 增量解码任意分块的 SSE 流,支持多行 data 拼接、CRLF、BOM、`[DONE]` 哨兵;
- 截断/解析失败一律返回 error → 调用方不上报 span(降级语义,不影响转发);
