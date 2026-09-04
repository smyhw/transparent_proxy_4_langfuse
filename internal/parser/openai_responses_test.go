// openai_responses_test.go 测试 OpenAI Responses API 解析器:JSON、SSE、
// 输入形态(字符串/数组)、failed 事件与 Match 边界。
package parser

import (
	"net/http"
	"strings"
	"testing"
)

// TestResponsesJSON 验证一次性 JSON 请求/响应的完整解析
func TestResponsesJSON(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","input":"你好"}`)
	resp := []byte(`{"id":"resp_1","model":"gpt-4o","status":"completed","output_text":"你好!","usage":{"input_tokens":8,"output_tokens":11}}`)
	p := &openaiResponsesParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/responses", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Provider != "openai" || res.Model != "gpt-4o" {
		t.Errorf("Provider/Model 错误: %s/%s", res.Provider, res.Model)
	}
	if res.Input != "你好" {
		t.Errorf("Input(字符串形态)= %q", res.Input)
	}
	if res.Output != "你好!" {
		t.Errorf("Output = %q", res.Output)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "stop" {
		t.Errorf("FinishReasons = %v(completed 应映射为 stop)", res.FinishReasons)
	}
	if res.Usage == nil || res.Usage.InputTokens != 8 || res.Usage.OutputTokens != 11 {
		t.Errorf("Usage = %+v", res.Usage)
	}
	if res.ResponseID != "resp_1" {
		t.Errorf("ResponseID = %q", res.ResponseID)
	}
}

// TestResponsesJSONInputArray 验证数组形态的 input 被序列化为 JSON 文本
func TestResponsesJSONInputArray(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","input":[{"type":"message","content":[{"type":"input_text","text":"你好"}]}]}`)
	resp := []byte(`{"id":"resp_2","model":"gpt-4o","status":"completed","output_text":"回复","usage":{"input_tokens":5,"output_tokens":2}}`)
	p := &openaiResponsesParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/responses", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if !strings.HasPrefix(res.Input, "[") || !strings.Contains(res.Input, "你好") {
		t.Errorf("Input(数组形态)= %q", res.Input)
	}
}

// TestResponsesSSE 验证 SSE 流式解析:created → deltas → completed
func TestResponsesSSE(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","stream":true,"input":"你好"}`)
	resp := []byte(strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_s","model":"gpt-4o","status":"in_progress"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","item_id":"i1","output_index":0,"delta":"你好"}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","item_id":"i1","output_index":0,"delta":"!流式"}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_s","status":"completed","output_text":"你好!流式","usage":{"input_tokens":8,"output_tokens":6}}}`,
		``,
	}, "\n"))
	p := &openaiResponsesParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/responses", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Output != "你好!流式" {
		t.Errorf("Output = %q", res.Output)
	}
	if !res.Stream {
		t.Errorf("应标记 Stream")
	}
	if res.ResponseID != "resp_s" || res.Model != "gpt-4o" {
		t.Errorf("ID/Model = %s/%s", res.ResponseID, res.Model)
	}
	if res.Usage == nil || res.Usage.InputTokens != 8 || res.Usage.OutputTokens != 6 {
		t.Errorf("Usage = %+v", res.Usage)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "stop" {
		t.Errorf("FinishReasons = %v", res.FinishReasons)
	}
}

// TestResponsesSSEDeltaOnly 验证无 completed 事件时按 output_index 排序拼接增量
func TestResponsesSSEDeltaOnly(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","stream":true,"input":"x"}`)
	resp := []byte(strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_d","model":"gpt-4o"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","item_id":"i1","output_index":0,"delta":"第一段"}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","item_id":"i2","output_index":1,"delta":"第二段"}`,
		``,
	}, "\n"))
	p := &openaiResponsesParser{}
	res, err := p.Parse(mkRecord(http.MethodPost, "/v1/responses", req, resp))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Output != "第一段\n第二段" {
		t.Errorf("Output = %q", res.Output)
	}
}

// TestResponsesSSEFailed 验证 response.failed 事件导致解析失败(降级不上报)
func TestResponsesSSEFailed(t *testing.T) {
	req := []byte(`{"model":"gpt-4o","stream":true,"input":"x"}`)
	resp := []byte(`event: response.failed` + "\n" + `data: {"type":"response.failed","response":{"id":"resp_f","status":"failed"}}` + "\n\n")
	p := &openaiResponsesParser{}
	if _, err := p.Parse(mkRecord(http.MethodPost, "/v1/responses", req, resp)); err == nil {
		t.Errorf("failed 事件应导致解析失败")
	}
}

// TestResponsesMatch 验证 header 级匹配边界
func TestResponsesMatch(t *testing.T) {
	p := &openaiResponsesParser{}
	if !p.Match(mkRecord(http.MethodPost, "/v1/responses", nil, nil)) {
		t.Errorf("POST /v1/responses 应匹配")
	}
	if p.Match(mkRecord(http.MethodPost, "/v1/chat/completions", nil, nil)) {
		t.Errorf("chat/completions 路径不应由 responses 匹配")
	}
	if p.Match(mkRecord(http.MethodGet, "/v1/responses", nil, nil)) {
		t.Errorf("GET 不应匹配")
	}
}
