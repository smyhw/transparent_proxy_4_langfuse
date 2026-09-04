// span.go 负责把流量快照 + 解析结果构建为 OTel span。
// 一次 LLM 调用 = 一条 trace = 一个 generation span;时间戳取自代理记录的真实
// 起止时刻(异步构建但延迟统计准确)。属性映射遵循 Langfuse 官方 OTel 文档。
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.36.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// tracerName 是 span 的埋点名称(体现本项目身份)
const tracerName = "transparent_proxy_4_langfuse"

// 字符串字面量属性键:semconv v1.36.0 中尚未收录,但 Langfuse 官方映射表要求上报的键
var (
	attrGenAIProviderName       = attribute.Key("gen_ai.provider.name")                // 新语义约定:供应商名
	attrGenAIRequestStream      = attribute.Key("gen_ai.request.stream")               // 是否流式请求
	attrGenAIPrompt             = attribute.Key("gen_ai.prompt")                       // → Langfuse input
	attrGenAICompletion         = attribute.Key("gen_ai.completion")                   // → Langfuse output
	attrGenAIInputMessages      = attribute.Key("gen_ai.input.messages")               // 完整输入消息(可选)
	attrGenAIOutputMessages     = attribute.Key("gen_ai.output.messages")              // 完整输出消息(可选)
	attrGenAITimeToFirstChunk   = attribute.Key("gen_ai.response.time_to_first_chunk") // 流式首块耗时(毫秒)
	attrLangfuseObservationType = attribute.Key("langfuse.observation.type")           // Langfuse 观测类型
	attrLangfuseTraceName       = attribute.Key("langfuse.trace.name")                 // Langfuse trace 名
	attrLangfuseUserID          = attribute.Key("langfuse.user.id")                    // Langfuse 用户 id
	attrLangfuseSessionID       = attribute.Key("langfuse.session.id")                 // Langfuse 会话 id
)

// bearerPrefix 是 Bearer 认证方案前缀(RFC 9110 认证方案名大小写不敏感)
const bearerPrefix = "Bearer "

// apiKeyFromHeaders 从快照请求头提取客户端 API key:优先 x-api-key;
// 否则取 Authorization,剥掉大小写不敏感的 "Bearer " 前缀,其余认证方案原样返回;
// 两个头都缺失(或均为空)时返回空串。
func apiKeyFromHeaders(h http.Header) string {
	if k := h.Get("X-Api-Key"); k != "" {
		return k
	}
	auth := h.Get("Authorization")
	if len(auth) >= len(bearerPrefix) && strings.EqualFold(auth[:len(bearerPrefix)], bearerPrefix) {
		return auth[len(bearerPrefix):]
	}
	return auth
}

// buildSpan 构建并上报单个 span(一次 LLM 调用的完整观测)。
// includeMessages 控制是否上报体积较大的完整消息数组属性。
func buildSpan(rec *record.Record, res *parser.Result, cfg *config.LangfuseConfig, includeMessages bool, tp *sdktrace.TracerProvider) {
	ctx, span := tp.Tracer(tracerName).Start(context.Background(), spanName(res),
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		oteltrace.WithTimestamp(rec.StartTime), // 真实请求开始时刻
		oteltrace.WithAttributes(buildAttributes(rec, res, cfg, includeMessages)...),
	)
	span.End(oteltrace.WithTimestamp(rec.EndTime)) // 真实响应结束时刻
	_ = ctx
}

// spanName 生成 span 名,遵循新版 GenAI 语义约定:"{操作} {模型}",如 "chat gpt-4o"
func spanName(res *parser.Result) string {
	if res.Model != "" {
		return fmt.Sprintf("%s %s", res.Operation, res.Model)
	}
	if res.Operation != "" {
		return res.Operation
	}
	return "chat"
}

// buildAttributes 组装 span 属性;逐条注释 Langfuse 映射关系
func buildAttributes(rec *record.Record, res *parser.Result, cfg *config.LangfuseConfig, includeMessages bool) []attribute.KeyValue {
	name := spanName(res)
	attrs := []attribute.KeyValue{
		semconv.GenAISystemKey.String(res.Provider),                      // Langfuse 用其识别生态(如 openai)
		semconv.GenAIOperationNameKey.String(res.Operation),              // 固定 "chat"
		attrGenAIProviderName.String(res.Provider),                       // 新语义约定:gen_ai.provider.name
		attrGenAIRequestStream.Bool(res.Stream),                          // 流式标记
		attrLangfuseObservationType.String("generation"),                 // 带 model 的 span 记作 generation
		attrLangfuseTraceName.String(firstNonEmpty(cfg.TraceName, name)), // trace 名(可配置覆盖)
	}
	if res.Model != "" {
		attrs = append(attrs, semconv.GenAIRequestModelKey.String(res.Model)) // → Langfuse model
	}
	if res.ResponseID != "" {
		attrs = append(attrs, semconv.GenAIResponseIDKey.String(res.ResponseID))
	}
	if len(res.FinishReasons) > 0 {
		attrs = append(attrs, semconv.GenAIResponseFinishReasonsKey.StringSlice(res.FinishReasons))
	}
	if res.Input != "" {
		attrs = append(attrs, attrGenAIPrompt.String(res.Input)) // → Langfuse input
	}
	if res.Output != "" {
		attrs = append(attrs, attrGenAICompletion.String(res.Output)) // → Langfuse output
	}
	if res.Usage != nil {
		// → Langfuse usage(缺失时不上报,不做估算避免污染成本统计)
		attrs = append(attrs,
			semconv.GenAIUsageInputTokensKey.Int64(res.Usage.InputTokens),
			semconv.GenAIUsageOutputTokensKey.Int64(res.Usage.OutputTokens),
		)
	}
	// 采样参数:有值才上报(→ Langfuse modelParameters)
	if res.Temperature != nil {
		attrs = append(attrs, semconv.GenAIRequestTemperatureKey.Float64(*res.Temperature))
	}
	if res.MaxTokens != nil {
		attrs = append(attrs, semconv.GenAIRequestMaxTokensKey.Int64(*res.MaxTokens))
	}
	if res.TopP != nil {
		attrs = append(attrs, semconv.GenAIRequestTopPKey.Float64(*res.TopP))
	}
	// 用户标识:优先级从高到低为 客户端 API key(需开启 user_key_as_user_id)→
	// 配置的请求头 → 请求体自带的 user 字段
	userID := ""
	if cfg.UserKeyAsUserID {
		userID = apiKeyFromHeaders(rec.RequestHeaders)
	}
	if userID == "" {
		userID = rec.RequestHeaders.Get(cfg.UserHeader)
	}
	if userID == "" {
		userID = res.UserID
	}
	if userID != "" {
		attrs = append(attrs, attrLangfuseUserID.String(userID))
	}
	// 会话标识:来自配置的请求头(同时映射 gen_ai.conversation.id 与 langfuse.session.id)
	if sessionID := rec.RequestHeaders.Get(cfg.SessionHeader); sessionID != "" {
		attrs = append(attrs,
			semconv.GenAIConversationIDKey.String(sessionID),
			attrLangfuseSessionID.String(sessionID),
		)
	}
	// 流式首块耗时:仅流式请求且确有首字节时上报(毫秒)
	if res.Stream && !rec.FirstResponseByte.IsZero() {
		ttfb := rec.FirstResponseByte.Sub(rec.StartTime).Milliseconds()
		if ttfb < 0 {
			ttfb = 0 // 时钟回拨保护
		}
		attrs = append(attrs, attrGenAITimeToFirstChunk.Int64(ttfb))
	}
	// 完整消息数组:体积大,仅按配置开启
	if includeMessages {
		if res.InputMessages != "" {
			attrs = append(attrs, attrGenAIInputMessages.String(res.InputMessages))
		}
		if res.OutputMessages != "" {
			attrs = append(attrs, attrGenAIOutputMessages.String(res.OutputMessages))
		}
	}
	return attrs
}

// firstNonEmpty 返回首个非空字符串(用于默认值回退)
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
