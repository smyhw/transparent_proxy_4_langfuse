// worker_test.go 端到端测试 worker 消费链路:队列 → 解析 → OTLP 导出。
// 用 httptest 模拟 OTLP 接收端,解码 protobuf 断言认证头与 span 属性。
package telemetry

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/queue"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// TestWorkerEndToEnd 验证完整链路:可解析记录被解析并导出到 OTLP 端点,
// 携带正确的 Basic Auth 与 ingestion-version 头;不可解析记录被静默降级。
func TestWorkerEndToEnd(t *testing.T) {
	// 模拟 OTLP 接收端:记录请求头并解码 protobuf
	var mu sync.Mutex
	var authHeader, ingestHeader, contentType string
	var received []*collectortracev1.ExportTraceServiceRequest
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读取导出请求失败: %v", err)
			return
		}
		var req collectortracev1.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("protobuf 解码失败: %v", err)
			return
		}
		mu.Lock()
		authHeader = r.Header.Get("Authorization")
		ingestHeader = r.Header.Get("x-langfuse-ingestion-version")
		contentType = r.Header.Get("Content-Type")
		received = append(received, &req)
		mu.Unlock()
		// 空导出响应(JSON 编码,符合 OTLP HTTP 协议)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	}))
	defer receiver.Close()

	// 构造指向 mock 接收端的配置
	cfg := config.Default()
	cfg.Langfuse = config.LangfuseConfig{
		Endpoint:         receiver.URL,
		PublicKey:        "pk-test",
		SecretKey:        "sk-test",
		IngestionVersion: 4,
		ServiceName:      "proxy-test",
		Environment:      "test",
		Batch: config.BatchConfig{
			Timeout:   config.Duration(50 * time.Millisecond), // 测试用短批量超时
			MaxSize:   512,
			QueueSize: 2048,
		},
		ExportTimeout: config.Duration(2 * time.Second),
		FlushTimeout:  config.Duration(2 * time.Second),
	}
	cfg.Queue.Workers = 2

	tp, err := NewTracerProvider(&cfg.Langfuse)
	if err != nil {
		t.Fatalf("创建 TracerProvider 失败: %v", err)
	}
	defer tp.Shutdown(context.Background())

	q := queue.NewQueue(8)
	reg := parser.NewRegistry(cfg.Parser.Enabled)
	w := NewWorker(q, reg, tp, cfg)
	w.Start()

	// 投递一条可解析的 OpenAI Chat 记录
	valid := &record.Record{
		Method:         http.MethodPost,
		URLPath:        "/v1/chat/completions",
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(time.Second),
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"你好"}]}`),
		ResponseBody:   []byte(`{"id":"chatcmpl-e2e","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"你好!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`),
	}
	// 投递一条不可解析的记录(非法 JSON → 降级不上报)
	invalid := &record.Record{
		Method:         http.MethodPost,
		URLPath:        "/v1/chat/completions",
		StartTime:      time.Now(),
		EndTime:        time.Now(),
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte("不是JSON"),
		ResponseBody:   []byte("也不是JSON"),
	}
	if !q.TryEnqueue(valid) || !q.TryEnqueue(invalid) {
		t.Fatalf("入队失败")
	}
	q.Close()
	w.Wait()

	// 强制刷出批量队列
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.ForceFlush(flushCtx); err != nil {
		t.Fatalf("ForceFlush 失败: %v", err)
	}

	// 断言认证头
	mu.Lock()
	defer mu.Unlock()
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("pk-test:sk-test"))
	if authHeader != wantAuth {
		t.Errorf("Authorization = %q,期望 %q", authHeader, wantAuth)
	}
	if ingestHeader != "4" {
		t.Errorf("x-langfuse-ingestion-version = %q,期望 \"4\"", ingestHeader)
	}
	if contentType != "application/x-protobuf" {
		t.Errorf("Content-Type = %q,期望 protobuf 编码", contentType)
	}
	// 断言 span 内容:应恰好导出 1 个 span(非法记录被降级)
	var spans int
	var sawPrompt, sawModel, sawObservationType bool
	for _, rs := range flattenResourceSpans(received) {
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				spans++
				if sp.Name != "chat gpt-4o" {
					t.Errorf("span 名 = %q", sp.Name)
				}
				for _, kv := range sp.Attributes {
					switch kv.Key {
					case "gen_ai.prompt":
						sawPrompt = kv.Value.GetStringValue() != ""
					case "gen_ai.request.model":
						sawModel = kv.Value.GetStringValue() == "gpt-4o"
					case "langfuse.observation.type":
						sawObservationType = kv.Value.GetStringValue() == "generation"
					}
				}
			}
		}
	}
	if spans != 1 {
		t.Errorf("导出 span 数 = %d,期望 1(非法记录应被降级)", spans)
	}
	if !sawPrompt || !sawModel || !sawObservationType {
		t.Errorf("关键属性缺失: prompt=%v model=%v observationType=%v", sawPrompt, sawModel, sawObservationType)
	}
	// 解析统计:1 命中解析器、1 成功、1 失败
	st := w.Stats()
	if st.Matched != 2 || st.Parsed != 1 || st.Failed != 1 {
		t.Errorf("worker 统计不符: %+v", st)
	}
}

// flattenResourceSpans 把多次导出请求中的 ResourceSpans 拍平
func flattenResourceSpans(reqs []*collectortracev1.ExportTraceServiceRequest) []*tracev1.ResourceSpans {
	var out []*tracev1.ResourceSpans
	for _, r := range reqs {
		out = append(out, r.ResourceSpans...)
	}
	return out
}

// TestWorkerNoTelemetry 验证 tp 为 nil(未启用 langfuse)时 worker 仍安全消费
func TestWorkerNoTelemetry(t *testing.T) {
	cfg := config.Default()
	q := queue.NewQueue(4)
	reg := parser.NewRegistry(cfg.Parser.Enabled)
	w := NewWorker(q, reg, nil, cfg)
	w.Start()
	q.TryEnqueue(&record.Record{
		Method:         http.MethodPost,
		URLPath:        "/v1/chat/completions",
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
		ResponseBody:   []byte(`{"id":"x","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`),
	})
	q.Close()
	w.Wait() // 不应 panic,不应挂死
}
