// anthropic_test.go 测试 Anthropic Messages 解析器:JSON、SSE 事件机、
// thinking 块忽略、error 事件与 Match 边界。
package parser

import (
	"net/http"
	"strings"
	"testing"
)

// TestAnthropicJSON 验证一次性 JSON 请求/响应的完整解析
func TestAnthropicJSON(t *testing.T) {
	req := []byte(`{"model":"claude-sonnet","max_tokens":100,"messages":[{"role":"user","content":"你好"}],"metadata":{"user_id":"u9"}}`)
	resp := []byte(`{"id":"msg_1","model":"claude-sonnet","content":[{"type":"text","text":"你好!"},{"type":"thinking","text":"内部推理"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5}}`)
	p := &anthropicParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/messages", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Provider != "anthropic" || res.Model != "claude-sonnet" {
		t.Errorf("Provider/Model 错误: %s/%s", res.Provider, res.Model)
	}
	// thinking 块刻意忽略,只取 text 块
	if res.Output != "你好!" {
		t.Errorf("Output = %q(thinking 块应被忽略)", res.Output)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "end_turn" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
	if res.Usage == nil || res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 {
		t.Errorf("Usage = %+v", res.Usage)
	}
	if res.ResponseID != "msg_1" {
		t.Errorf("ResponseID = %q", res.ResponseID)
	}
	if res.MaxTokens == nil || *res.MaxTokens != 100 {
		t.Errorf("MaxTokens = %v", res.MaxTokens)
	}
	if res.UserID != "u9" {
		t.Errorf("UserID(来自 metadata)= %q", res.UserID)
	}
	if !strings.Contains(res.Input, "你好") {
		t.Errorf("Input = %q", res.Input)
	}
}

// TestAnthropicSSE 验证 SSE 事件机全序列:message_start → delta → message_delta
func TestAnthropicSSE(t *testing.T) {
	req := []byte(`{"model":"claude-sonnet","stream":true,"messages":[{"role":"user","content":"你好"}]}`)
	resp := []byte(strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"msg_s","model":"claude-sonnet","usage":{"input_tokens":10}}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"你好"}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"!流式"}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":""}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"推理过程"}}`,
		``,
		`event: content_block_stop`,
		`data: {"type":"content_block_stop","index":1}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}`,
		``,
		`event: message_stop`,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n"))
	p := &anthropicParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/messages", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Output != "你好!流式" {
		t.Errorf("Output = %q(thinking 增量应被忽略)", res.Output)
	}
	if !res.Stream {
		t.Errorf("应标记 Stream")
	}
	if res.ResponseID != "msg_s" || res.Model != "claude-sonnet" {
		t.Errorf("ID/Model = %s/%s", res.ResponseID, res.Model)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "end_turn" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
	// 输入 usage 来自 message_start,输出 usage 来自 message_delta
	if res.Usage == nil || res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 {
		t.Errorf("Usage = %+v", res.Usage)
	}
}

// TestAnthropicSSEError 验证 error 事件导致解析失败(降级不上报)
func TestAnthropicSSEError(t *testing.T) {
	req := []byte(`{"model":"claude-sonnet","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	resp := []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"过载\"}}\n\n")
	p := &anthropicParser{}
	if _, err := p.Parse(mkRecord(http.MethodPost, "/v1/messages", req, resp)); err == nil {
		t.Errorf("error 事件应导致解析失败")
	}
}

// TestAnthropicMatch 验证 header 级匹配边界
func TestAnthropicMatch(t *testing.T) {
	p := &anthropicParser{}
	if !p.Match(mkRecord(http.MethodPost, "/v1/messages", nil, nil)) {
		t.Errorf("POST /v1/messages 应匹配")
	}
	if p.Match(mkRecord(http.MethodGet, "/v1/messages", nil, nil)) {
		t.Errorf("GET 不应匹配")
	}
	if p.Match(mkRecord(http.MethodPost, "/v1/chat/completions", nil, nil)) {
		t.Errorf("chat/completions 路径不应由 anthropic 匹配")
	}
}
