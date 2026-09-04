// proxy_test.go 测试转发层:透传一致性、SSE flush 时序、截断降级、
// 候选/非候选行为与升级请求判定。
package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/queue"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// newTestProxy 构建指向指定上游的测试代理(启用 langfuse 密钥以激活捕获)
func newTestProxy(t *testing.T, upstreamURL string, mutate func(*config.Config)) (*Proxy, *queue.Queue) {
	t.Helper()
	cfg := config.Default()
	cfg.Target.URL = upstreamURL
	cfg.Langfuse.PublicKey = "pk-test"
	cfg.Langfuse.SecretKey = "sk-test"
	if mutate != nil {
		mutate(cfg)
	}
	reg := parser.NewRegistry(cfg.Parser.Enabled)
	q := queue.NewQueue(cfg.Queue.Size)
	p, err := New(cfg, q, reg)
	if err != nil {
		t.Fatalf("创建代理失败: %v", err)
	}
	return p, q
}

// tryRecv 从队列收取一条记录(带超时,避免测试挂死)
func tryRecv(t *testing.T, q *queue.Queue) *record.Record {
	t.Helper()
	select {
	case rec := <-q.Ch():
		return rec
	case <-time.After(2 * time.Second):
		t.Fatalf("等待队列记录超时")
		return nil
	}
}

// TestPassthroughJSON 验证 JSON 请求/响应经代理后与直连上游逐字节一致
func TestPassthroughJSON(t *testing.T) {
	// 上游:回显请求体、指定头与状态码
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Echo-Path", r.URL.Path)
		w.Header().Set("X-Echo-Forwarded", r.Header.Get("X-Forwarded-For"))
		w.WriteHeader(http.StatusCreated)
		w.Write(body)
	}))
	defer upstream.Close()

	p, _ := newTestProxy(t, upstream.URL, nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	payload := `{"model":"gpt-4o","messages":[{"role":"user","content":"你好"}]}`
	resp, err := http.Post(proxySrv.URL+"/v1/chat/completions", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("状态码 = %d,期望 201", resp.StatusCode)
	}
	if string(got) != payload {
		t.Errorf("响应体不一致: %q", got)
	}
	if resp.Header.Get("X-Echo-Path") != "/v1/chat/completions" {
		t.Errorf("路径透传失败: %q", resp.Header.Get("X-Echo-Path"))
	}
	// 标准代理行为:X-Forwarded-For 已设置
	if resp.Header.Get("X-Echo-Forwarded") == "" {
		t.Errorf("X-Forwarded-For 未设置")
	}
}

// TestSSEFlushTiming 验证流式响应的逐块 flush 透传:首块在流结束前到达
func TestSSEFlushTiming(t *testing.T) {
	// 上游:写首块+flush,延时后再写尾块(期间不结束流)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		w.Write([]byte("data: 首块\n\n"))
		flusher.Flush()
		time.Sleep(300 * time.Millisecond) // 模拟生成间隔
		w.Write([]byte("data: 尾块\n\ndata: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	p, q := newTestProxy(t, upstream.URL, nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	start := time.Now()
	resp, err := http.Post(proxySrv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// 读取首块:若代理把流整体缓冲,这里要等 300ms 才能读到
	first := make([]byte, 32)
	n, err := resp.Body.Read(first)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 250*time.Millisecond {
		t.Errorf("首块到达耗时 %v,超过流内延时,说明 Flush 未透传(流被缓冲)", elapsed)
	}
	// 读完剩余部分
	rest, _ := io.ReadAll(resp.Body)
	full := string(first[:n]) + string(rest)
	if !strings.Contains(full, "尾块") {
		t.Errorf("流内容不完整: %q", full)
	}
	// 捕获的响应体应包含完整 SSE 流
	rec := tryRecv(t, q)
	if !strings.Contains(string(rec.ResponseBody), "data: 首块") || !strings.Contains(string(rec.ResponseBody), "[DONE]") {
		t.Errorf("捕获内容不完整")
	}
	if rec.FirstResponseByte.IsZero() {
		t.Errorf("应记录首字节时刻")
	}
}

// TestTruncatedStillForwarded 验证超上限截断只影响捕获,转发内容完整
func TestTruncatedStillForwarded(t *testing.T) {
	big := strings.Repeat("x", 4096) // 4KB 响应体
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(big))
	}))
	defer upstream.Close()

	// 捕获上限压到 64 字节
	p, q := newTestProxy(t, upstream.URL, func(c *config.Config) {
		c.Capture.MaxResponseBytes = 64
	})
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	resp, err := http.Post(proxySrv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	// 客户端收到的内容必须完整
	if string(got) != big {
		t.Errorf("转发内容不完整: 收到 %d 字节,期望 %d", len(got), len(big))
	}
	// 捕获侧被截断
	rec := tryRecv(t, q)
	if !rec.ResponseTruncated || len(rec.ResponseBody) != 64 {
		t.Errorf("截断标记/捕获大小错误: truncated=%v len=%d", rec.ResponseTruncated, len(rec.ResponseBody))
	}
}

// TestNonCandidateNoRecord 验证非候选请求零捕获零入队
func TestNonCandidateNoRecord(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	p, q := newTestProxy(t, upstream.URL, nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	// GET 到候选路径:方法不匹配 → 不捕获
	if _, err := http.Get(proxySrv.URL + "/v1/chat/completions"); err != nil {
		t.Fatal(err)
	}
	// POST 到无关路径:路径不匹配 → 不捕获
	if _, err := http.Post(proxySrv.URL+"/other/path", "application/json", strings.NewReader(`{}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-q.Ch():
		t.Fatalf("非候选请求不应入队,收到: %+v", rec)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestCandidateRecord 验证候选请求入队且快照字段完整
func TestCandidateRecord(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"r1"}`))
	}))
	defer upstream.Close()

	p, q := newTestProxy(t, upstream.URL, nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	payload := `{"model":"m","messages":[{"role":"user","content":"hi"}]}`
	resp, err := http.Post(proxySrv.URL+"/v1/chat/completions", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	rec := tryRecv(t, q)
	if rec.Method != http.MethodPost || rec.URLPath != "/v1/chat/completions" {
		t.Errorf("快照方法/路径错误: %s %s", rec.Method, rec.URLPath)
	}
	if string(rec.RequestBody) != payload {
		t.Errorf("请求体快照错误: %q", rec.RequestBody)
	}
	if string(rec.ResponseBody) != `{"id":"r1"}` {
		t.Errorf("响应体快照错误: %q", rec.ResponseBody)
	}
	if rec.StatusCode != http.StatusOK {
		t.Errorf("状态码 = %d", rec.StatusCode)
	}
	if rec.StartTime.IsZero() || rec.EndTime.IsZero() || rec.EndTime.Before(rec.StartTime) {
		t.Errorf("时间戳错误: %v ~ %v", rec.StartTime, rec.EndTime)
	}
}

// TestIsCandidate 验证候选判定的各条边界(升级请求、排除路径、未启用等)
func TestIsCandidate(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, _ := newTestProxy(t, upstream.URL, nil)

	mkReq := func(method, path, contentType string) *http.Request {
		r := httptest.NewRequest(method, "http://proxy"+path, nil)
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		return r
	}

	if !p.isCandidate(mkReq(http.MethodPost, "/v1/chat/completions", "application/json")) {
		t.Errorf("标准候选请求应命中")
	}
	if !p.isCandidate(mkReq(http.MethodPost, "/v1/messages", "application/json; charset=utf-8")) {
		t.Errorf("带 charset 的 Content-Type 应命中")
	}
	if p.isCandidate(mkReq(http.MethodGet, "/v1/chat/completions", "application/json")) {
		t.Errorf("GET 不应命中")
	}
	if p.isCandidate(mkReq(http.MethodPost, "/v1/chat/completions", "text/plain")) {
		t.Errorf("text/plain 不应命中")
	}
	// 升级请求(WebSocket 握手)不应命中
	up := mkReq(http.MethodPost, "/v1/chat/completions", "application/json")
	up.Header.Set("Connection", "Upgrade")
	up.Header.Set("Upgrade", "websocket")
	if p.isCandidate(up) {
		t.Errorf("升级请求不应命中")
	}
	// 未启用 langfuse 时一律不命中
	disabled, _ := newTestProxy(t, upstream.URL, func(c *config.Config) {
		c.Langfuse.PublicKey = ""
		c.Langfuse.SecretKey = ""
	})
	if disabled.isCandidate(mkReq(http.MethodPost, "/v1/chat/completions", "application/json")) {
		t.Errorf("未启用 langfuse 时不应命中")
	}
}

// TestPathIncludeExclude 验证配置的额外候选路径与排除路径
func TestPathIncludeExclude(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, _ := newTestProxy(t, upstream.URL, func(c *config.Config) {
		c.Parser.PathInclude = []string{"/v1/custom/llm"}
		c.Parser.PathExclude = []string{"/v1/chat/completions"}
	})
	mkReq := func(path string) *http.Request {
		return httptest.NewRequest(http.MethodPost, "http://proxy"+path, nil)
	}
	if !p.isCandidate(mkReq("/v1/custom/llm")) {
		t.Errorf("path_include 的路径应命中")
	}
	if p.isCandidate(mkReq("/v1/chat/completions")) {
		t.Errorf("path_exclude 的路径不应命中")
	}
}

// TestErrorHandler 验证上游不可达时返回 502 且不 panic
func TestErrorHandler(t *testing.T) {
	// 指向必然失败的地址
	p, q := newTestProxy(t, "http://127.0.0.1:1", nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()
	resp, err := http.Post(proxySrv.URL+"/v1/chat/completions", "application/json", bytes.NewReader([]byte(`{"model":"m"}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("状态码 = %d,期望 502", resp.StatusCode)
	}
	// 失败请求同样入队(worker 会因空响应体解析失败而丢弃,属预期降级)
	rec := tryRecv(t, q)
	if rec.StatusCode != http.StatusBadGateway {
		t.Errorf("快照状态码 = %d", rec.StatusCode)
	}
}

// TestSnapshotIncludesAPIKeyWhenEnabled 验证开启 user_key_as_user_id 后认证头进入快照(原始值,不剥离)
func TestSnapshotIncludesAPIKeyWhenEnabled(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"r1"}`))
	}))
	defer upstream.Close()

	p, q := newTestProxy(t, upstream.URL, func(c *config.Config) {
		c.Langfuse.UserKeyAsUserID = true
	})
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	req, err := http.NewRequest(http.MethodPost, proxySrv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", "k1")
	req.Header.Set("Authorization", "Bearer sk-abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	rec := tryRecv(t, q)
	if got := rec.RequestHeaders.Get("X-Api-Key"); got != "k1" {
		t.Errorf("快照 X-Api-Key = %q,期望 k1", got)
	}
	if got := rec.RequestHeaders.Get("Authorization"); got != "Bearer sk-abc" {
		t.Errorf("快照 Authorization = %q,期望 Bearer sk-abc(原始值)", got)
	}
}

// TestSnapshotExcludesAPIKeyByDefault 验证默认配置下认证头不进快照(隐私默认)
func TestSnapshotExcludesAPIKeyByDefault(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"r1"}`))
	}))
	defer upstream.Close()

	p, q := newTestProxy(t, upstream.URL, nil)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	req, err := http.NewRequest(http.MethodPost, proxySrv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", "k1")
	req.Header.Set("Authorization", "Bearer sk-abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	rec := tryRecv(t, q)
	if got := rec.RequestHeaders.Get("X-Api-Key"); got != "" {
		t.Errorf("默认配置下快照不应包含 X-Api-Key,实际 %q", got)
	}
	if got := rec.RequestHeaders.Get("Authorization"); got != "" {
		t.Errorf("默认配置下快照不应包含 Authorization,实际 %q", got)
	}
}
