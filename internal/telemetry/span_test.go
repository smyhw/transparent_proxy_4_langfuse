// span_test.go 测试 span 构建:名称、kind、时间戳与属性映射。
package telemetry

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// testCfg 返回测试用的 Langfuse 配置
func testCfg() *config.LangfuseConfig {
	cfg := config.Default().Langfuse
	return &cfg
}

// findAttr 从属性列表中按键查找;不存在返回 false
func findAttr(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// TestBuildSpan 验证 span 名称、kind、时间戳与核心属性
func TestBuildSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	start := time.Now().Add(-2 * time.Second)
	end := start.Add(1500 * time.Millisecond)
	rec := &record.Record{
		URLPath:           "/v1/chat/completions",
		StartTime:         start,
		EndTime:           end,
		FirstResponseByte: start.Add(300 * time.Millisecond),
		RequestHeaders:    map[string][]string{"X-User": {"u42"}},
	}
	res := &parser.Result{
		Provider:      "openai",
		Operation:     "chat",
		Model:         "gpt-4o",
		Stream:        true,
		Input:         "输入内容",
		Output:        "输出内容",
		FinishReasons: []string{"stop"},
		ResponseID:    "resp-1",
		Usage:         &parser.Usage{InputTokens: 10, OutputTokens: 5},
	}
	cfg := testCfg()
	cfg.UserHeader = "X-User"
	buildSpan(rec, res, cfg, false, tp)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("span 数 = %d,期望 1", len(spans))
	}
	sp := spans[0]
	if sp.Name() != "chat gpt-4o" {
		t.Errorf("span 名 = %q", sp.Name())
	}
	if sp.SpanKind() != oteltrace.SpanKindClient {
		t.Errorf("kind = %v,期望 CLIENT", sp.SpanKind())
	}
	if !sp.StartTime().Equal(start) || !sp.EndTime().Equal(end) {
		t.Errorf("时间戳错误: %v ~ %v", sp.StartTime(), sp.EndTime())
	}
	attrs := sp.Attributes()
	checks := map[string]string{
		"gen_ai.system":             "openai",
		"gen_ai.provider.name":      "openai",
		"gen_ai.operation.name":     "chat",
		"gen_ai.request.model":      "gpt-4o",
		"gen_ai.response.id":        "resp-1",
		"gen_ai.prompt":             "输入内容",
		"gen_ai.completion":         "输出内容",
		"langfuse.observation.type": "generation",
		"langfuse.trace.name":       "chat gpt-4o",
		"langfuse.user.id":          "u42",
	}
	for k, want := range checks {
		v, ok := findAttr(attrs, k)
		if !ok {
			t.Errorf("缺少属性 %s", k)
			continue
		}
		if v.AsString() != want {
			t.Errorf("属性 %s = %q,期望 %q", k, v.AsString(), want)
		}
	}
	// finish_reasons 为字符串数组
	if v, ok := findAttr(attrs, "gen_ai.response.finish_reasons"); !ok || len(v.AsStringSlice()) != 1 || v.AsStringSlice()[0] != "stop" {
		t.Errorf("finish_reasons 属性错误: %v", v)
	}
	// usage 数值
	if v, ok := findAttr(attrs, "gen_ai.usage.input_tokens"); !ok || v.AsInt64() != 10 {
		t.Errorf("input_tokens 属性错误")
	}
	if v, ok := findAttr(attrs, "gen_ai.usage.output_tokens"); !ok || v.AsInt64() != 5 {
		t.Errorf("output_tokens 属性错误")
	}
	// 流式首块耗时(毫秒)
	if v, ok := findAttr(attrs, "gen_ai.response.time_to_first_chunk"); !ok || v.AsInt64() != 300 {
		t.Errorf("首块耗时属性错误: %v", v)
	}
	// include_messages 关闭时不上报完整消息
	if _, ok := findAttr(attrs, "gen_ai.input.messages"); ok {
		t.Errorf("include_messages=false 时不应上报 input.messages")
	}
}

// TestBuildSpanNoUsage 验证 Usage 为 nil 时不上报 usage 属性(不做估算)
func TestBuildSpanNoUsage(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	now := time.Now()
	rec := &record.Record{StartTime: now, EndTime: now.Add(time.Second)}
	res := &parser.Result{Provider: "openai", Operation: "chat", Model: "m", Input: "in", Output: "out"}
	buildSpan(rec, res, testCfg(), false, tp)

	sp := sr.Ended()[0]
	if _, ok := findAttr(sp.Attributes(), "gen_ai.usage.input_tokens"); ok {
		t.Errorf("Usage 缺失时不应上报 usage 属性")
	}
}

// TestBuildSpanTraceNameOverride 验证 trace_name 静态覆盖生效
func TestBuildSpanTraceNameOverride(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	now := time.Now()
	rec := &record.Record{StartTime: now, EndTime: now.Add(time.Second)}
	res := &parser.Result{Provider: "openai", Operation: "chat", Model: "m", Input: "in", Output: "out"}
	cfg := testCfg()
	cfg.TraceName = "自定义trace名"
	buildSpan(rec, res, cfg, false, tp)

	sp := sr.Ended()[0]
	if v, ok := findAttr(sp.Attributes(), "langfuse.trace.name"); !ok || v.AsString() != "自定义trace名" {
		t.Errorf("trace_name 覆盖失败: %v", v)
	}
}

// TestBuildSpanIncludeMessages 验证 include_messages 开启时上报完整消息
func TestBuildSpanIncludeMessages(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	now := time.Now()
	rec := &record.Record{StartTime: now, EndTime: now.Add(time.Second)}
	res := &parser.Result{
		Provider:       "openai",
		Operation:      "chat",
		Model:          "m",
		Input:          "in",
		Output:         "out",
		InputMessages:  `[{"role":"user","content":"hi"}]`,
		OutputMessages: `[{"role":"assistant","content":"hi"}]`,
	}
	buildSpan(rec, res, testCfg(), true, tp)

	sp := sr.Ended()[0]
	if v, ok := findAttr(sp.Attributes(), "gen_ai.input.messages"); !ok || v.AsString() == "" {
		t.Errorf("include_messages=true 时应上报 input.messages")
	}
}

// TestSpanName 验证 span 名生成的回退逻辑
func TestSpanName(t *testing.T) {
	cases := []struct {
		res  *parser.Result
		want string
	}{
		{&parser.Result{Operation: "chat", Model: "gpt-4o"}, "chat gpt-4o"},
		{&parser.Result{Operation: "chat"}, "chat"},
		{&parser.Result{}, "chat"},
	}
	for _, c := range cases {
		if got := spanName(c.res); got != c.want {
			t.Errorf("spanName(%+v) = %q,期望 %q", c.res, got, c.want)
		}
	}
}

// TestAPIKeyFromHeaders 验证客户端 API key 提取:x-api-key 优先、Bearer 前缀剥离等
func TestAPIKeyFromHeaders(t *testing.T) {
	cases := []struct {
		name    string
		headers http.Header
		want    string
	}{
		{"仅 x-api-key", http.Header{"X-Api-Key": {"k1"}}, "k1"},
		{"x-api-key 优先于 Authorization", http.Header{"X-Api-Key": {"k1"}, "Authorization": {"Bearer k2"}}, "k1"},
		{"Bearer 剥离", http.Header{"Authorization": {"Bearer sk-abc"}}, "sk-abc"},
		{"小写 bearer 同样剥离", http.Header{"Authorization": {"bearer sk-abc"}}, "sk-abc"},
		{"非 Bearer 方案原样", http.Header{"Authorization": {"Basic dXNlcjpwYXNz"}}, "Basic dXNlcjpwYXNz"},
		{"Token 方案原样", http.Header{"Authorization": {"Token abc"}}, "Token abc"},
		{"仅前缀无凭证", http.Header{"Authorization": {"Bearer "}}, ""},
		{"空 x-api-key 回落 Authorization", http.Header{"X-Api-Key": {""}, "Authorization": {"Bearer k3"}}, "k3"},
		{"两个头都无", http.Header{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := apiKeyFromHeaders(c.headers); got != c.want {
				t.Errorf("apiKeyFromHeaders(%v) = %q,期望 %q", c.headers, got, c.want)
			}
		})
	}
}

// TestBuildSpanUserKeyPriority 验证用户 id 优先级:开启后 API key > user_header >
// 请求体 user 字段;默认关闭时认证头不被采用。
func TestBuildSpanUserKeyPriority(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		headers  http.Header
		bodyUser string
		want     string
	}{
		{"key 压过 user_header 与 body user", true, http.Header{"X-Api-Key": {"k1"}, "X-User": {"u42"}}, "body-user", "k1"},
		{"Bearer 剥离后作为用户 id", true, http.Header{"Authorization": {"Bearer k2"}, "X-User": {"u42"}}, "body-user", "k2"},
		{"x-api-key 优先于 Authorization", true, http.Header{"X-Api-Key": {"k1"}, "Authorization": {"Bearer k2"}}, "", "k1"},
		{"无认证头回落 user_header", true, http.Header{"X-User": {"u42"}}, "body-user", "u42"},
		{"无认证头无配置头回落 body user", true, http.Header{}, "body-user", "body-user"},
		{"默认关闭不采用 key", false, http.Header{"X-Api-Key": {"k1"}, "X-User": {"u42"}}, "", "u42"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sr := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
			defer tp.Shutdown(context.Background())
			now := time.Now()
			rec := &record.Record{
				StartTime:      now,
				EndTime:        now.Add(time.Second),
				RequestHeaders: c.headers,
			}
			res := &parser.Result{Provider: "openai", Operation: "chat", Model: "m", UserID: c.bodyUser}
			cfg := testCfg()
			cfg.UserKeyAsUserID = c.enabled
			cfg.UserHeader = "X-User"
			buildSpan(rec, res, cfg, false, tp)
			sp := sr.Ended()[0]
			v, ok := findAttr(sp.Attributes(), "langfuse.user.id")
			if c.want == "" {
				if ok {
					t.Errorf("不应上报 langfuse.user.id,实际 = %q", v.AsString())
				}
				return
			}
			if !ok || v.AsString() != c.want {
				t.Errorf("langfuse.user.id = %v,期望 %q", v, c.want)
			}
		})
	}
}
