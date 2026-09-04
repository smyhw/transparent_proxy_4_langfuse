// openai_chat_test.go 测试 OpenAI Chat Completions 解析器:JSON、SSE 重建、
// 工具调用合并、gzip 解压、截断降级与 Match 边界。
package parser

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// mkRecord 构造测试用流量快照(请求 Content-Type 默认 application/json)
func mkRecord(method, path string, reqBody, respBody []byte) *record.Record {
	return &record.Record{
		Method:          method,
		URLPath:         path,
		RequestHeaders:  http.Header{"Content-Type": {"application/json"}},
		ResponseHeaders: http.Header{},
		RequestBody:     reqBody,
		ResponseBody:    respBody,
	}
}

// TestOpenAIChatJSON 验证一次性 JSON 请求/响应的完整解析
func TestOpenAIChatJSON(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"你好"}],"temperature":0.7,"max_completion_tokens":100,"user":"u1"}`)
	resp := []byte(`{"id":"chatcmpl-1","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"你好!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`)
	p := &openaiChatParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/chat/completions", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Provider != "openai" || res.Model != "gpt-4o" {
		t.Errorf("Provider/Model 错误: %s/%s", res.Provider, res.Model)
	}
	if res.Output != "你好!" {
		t.Errorf("Output = %q", res.Output)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "stop" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
	if res.Usage == nil || res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 {
		t.Errorf("Usage = %+v", res.Usage)
	}
	if res.ResponseID != "chatcmpl-1" {
		t.Errorf("ResponseID = %q", res.ResponseID)
	}
	if res.Temperature == nil || *res.Temperature != 0.7 {
		t.Errorf("Temperature = %v", res.Temperature)
	}
	if res.MaxTokens == nil || *res.MaxTokens != 100 {
		t.Errorf("MaxTokens = %v", res.MaxTokens)
	}
	if res.UserID != "u1" {
		t.Errorf("UserID = %q", res.UserID)
	}
	if !strings.Contains(res.Input, "你好") || !strings.HasPrefix(res.Input, "[") {
		t.Errorf("Input = %q", res.Input)
	}
	if res.Stream {
		t.Errorf("非流式请求不应标记 Stream")
	}
}

// TestOpenAIChatSSE 验证 SSE 流式响应重建:文本拼接、finish、usage、[DONE] 哨兵
func TestOpenAIChatSSE(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"你好"}]}`)
	resp := []byte(strings.Join([]string{
		`data: {"id":"chatcmpl-s","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"你好"}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{"content":"!我是"}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{"content":"流式回复。"}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":7}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	p := &openaiChatParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/chat/completions", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Output != "你好!我是流式回复。" {
		t.Errorf("Output = %q", res.Output)
	}
	if !res.Stream {
		t.Errorf("应标记 Stream")
	}
	if res.ResponseID != "chatcmpl-s" || res.Model != "gpt-4o" {
		t.Errorf("ID/Model = %s/%s", res.ResponseID, res.Model)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "stop" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
	if res.Usage == nil || res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 7 {
		t.Errorf("Usage = %+v", res.Usage)
	}
}

// TestOpenAIChatSSEToolCalls 验证流式工具调用按 index 合并
func TestOpenAIChatSSEToolCalls(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"天气?"}]}`)
	resp := []byte(strings.Join([]string{
		`data: {"id":"chatcmpl-t","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"北京\"}"}}]}}]}`,
		``,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	p := &openaiChatParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/chat/completions", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	// 反序列化 Output 中的工具调用数组,断言分片已合并为完整 JSON
	var calls []chatToolCall
	if err := json.Unmarshal([]byte(res.Output), &calls); err != nil {
		t.Fatalf("Output 不是合法工具调用 JSON: %v (%q)", err, res.Output)
	}
	if len(calls) != 1 || calls[0].Name != "get_weather" || calls[0].ID != "call_1" {
		t.Errorf("工具调用合并错误: %+v", calls)
	}
	if calls[0].Arguments != `{"city":"北京"}` {
		t.Errorf("参数分片未拼接完整: %q", calls[0].Arguments)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "tool_calls" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
	// 结构化输出应包含 assistant 消息与 tool_calls
	if !strings.Contains(res.OutputMessages, "tool_calls") {
		t.Errorf("OutputMessages 未包含工具调用: %q", res.OutputMessages)
	}
}

// TestOpenAIChatGzipResponse 验证 gzip 压缩的响应体能被正确解压解析
func TestOpenAIChatGzipResponse(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`)
	plain := []byte(`{"id":"chatcmpl-g","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"压缩回复"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`)
	// gzip 压缩响应体
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(plain)
	zw.Close()
	rec := mkRecord(http.MethodPost, "/v1/chat/completions", req, buf.Bytes())
	rec.ResponseHeaders.Set("Content-Encoding", "gzip")
	p := &openaiChatParser{}
	res, err := p.Parse(rec)
	if err != nil {
		t.Fatalf("gzip 响应解析失败: %v", err)
	}
	if res.Output != "压缩回复" || res.Usage == nil || res.Usage.OutputTokens != 3 {
		t.Errorf("gzip 解析结果错误: %+v", res)
	}
}

// TestOpenAIChatTruncated 验证截断流量降级(返回错误,不上报)
func TestOpenAIChatTruncated(t *testing.T) {
	p := &openaiChatParser{}
	rec := mkRecord(http.MethodPost, "/v1/chat/completions", nil, []byte(`{"id":"x"}`))
	rec.ResponseTruncated = true
	if _, err := p.Parse(rec); err == nil {
		t.Errorf("截断流量应返回错误")
	}
}

// TestOpenAIChatMatch 验证 header 级匹配边界
func TestOpenAIChatMatch(t *testing.T) {
	p := &openaiChatParser{}
	cases := []struct {
		name string
		rec  *record.Record
		want bool
	}{
		{"正常 POST", mkRecord(http.MethodPost, "/v1/chat/completions", nil, nil), true},
		{"GET 不匹配", mkRecord(http.MethodGet, "/v1/chat/completions", nil, nil), false},
		{"路径不匹配", mkRecord(http.MethodPost, "/v1/completions", nil, nil), false},
		{"Azure 前缀路径", mkRecord(http.MethodPost, "/openai/deployments/x/chat/completions", nil, nil), true},
		{"Content-Type 缺失放行", mkRecord(http.MethodPost, "/v1/chat/completions", nil, nil), true},
		{"非 JSON Content-Type", func() *record.Record {
			rec := mkRecord(http.MethodPost, "/v1/chat/completions", nil, nil)
			rec.RequestHeaders.Set("Content-Type", "text/plain")
			return rec
		}(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := p.Match(c.rec); got != c.want {
				t.Errorf("Match = %v,期望 %v", got, c.want)
			}
		})
	}
}

// TestOpenAIChatInvalidJSON 验证非法 JSON 返回错误
func TestOpenAIChatInvalidJSON(t *testing.T) {
	p := &openaiChatParser{}
	rec := mkRecord(http.MethodPost, "/v1/chat/completions", []byte("不是JSON"), []byte("也不是JSON"))
	if _, err := p.Parse(rec); err == nil {
		t.Errorf("非法 JSON 应返回错误")
	}
}
