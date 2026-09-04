// openai_responses.go 实现 OpenAI Responses API 协议解析器(/v1/responses)。
// 兼容 JSON 与 SSE(流式)两种响应;SSE 按事件类型处理
// (response.created / response.output_text.delta / response.completed 等)。
package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// openaiResponsesParser 是 OpenAI Responses API 协议解析器
type openaiResponsesParser struct{}

// Name 返回解析器名称
func (p *openaiResponsesParser) Name() string { return "openai_responses" }

// PathSuffixes 返回该解析器覆盖的路径后缀
func (p *openaiResponsesParser) PathSuffixes() []string { return []string{"/v1/responses"} }

// Match 依据方法、路径后缀与 Content-Type 判定(header 级,极快)
func (p *openaiResponsesParser) Match(r *record.Record) bool {
	return r.Method == http.MethodPost &&
		strings.HasSuffix(r.URLPath, "/v1/responses") &&
		AcceptsJSON(r.RequestHeaders)
}

// responsesRequest 是 Responses API 请求体的解析子集
type responsesRequest struct {
	Model           string          `json:"model"`
	Input           json.RawMessage `json:"input"` // 字符串或条目数组
	Stream          bool            `json:"stream"`
	Temperature     *float64        `json:"temperature"`
	TopP            *float64        `json:"top_p"`
	MaxOutputTokens *int64          `json:"max_output_tokens"`
	User            string          `json:"user"`
}

// responsesUsage 是 Responses API 的 usage 结构
type responsesUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// Parse 解析请求与响应,产出归一化结果;失败返回 error(调用方不上报)
func (p *openaiResponsesParser) Parse(r *record.Record) (*Result, error) {
	if err := rejectTruncated(r); err != nil {
		return nil, err
	}
	reqBody, err := decompress(r.RequestBody, r.RequestHeaders)
	if err != nil {
		return nil, fmt.Errorf("请求体解压失败: %w", err)
	}
	respBody, err := decompress(r.ResponseBody, r.ResponseHeaders)
	if err != nil {
		return nil, fmt.Errorf("响应体解压失败: %w", err)
	}

	// 解析请求体
	var req responsesRequest
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return nil, fmt.Errorf("请求体不是合法 JSON: %w", err)
	}
	res := &Result{
		Provider:    "openai",
		Operation:   "chat",
		Model:       req.Model,
		Stream:      req.Stream,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   req.MaxOutputTokens,
		UserID:      req.User,
	}
	// 输入内容:input 可能是纯字符串,也可能是条目数组
	if len(req.Input) > 0 {
		var s string
		if json.Unmarshal(req.Input, &s) == nil {
			res.Input = s
		} else {
			res.Input = string(req.Input)
		}
		res.InputMessages = res.Input
	}

	if err := p.parseResponse(res, respBody); err != nil {
		return nil, err
	}
	if res.Model == "" {
		return nil, errors.New("缺少模型名,放弃解析")
	}
	return res, nil
}

// parseResponse 按响应体形态分发到 JSON 或 SSE 解析
func (p *openaiResponsesParser) parseResponse(res *Result, body []byte) error {
	if looksLikeJSON(body) {
		return p.parseJSONResponse(res, body)
	}
	return p.parseSSEResponse(res, body)
}

// parseJSONResponse 解析一次性(非流式)JSON 响应
func (p *openaiResponsesParser) parseJSONResponse(res *Result, body []byte) error {
	var resp struct {
		ID         string            `json:"id"`
		Model      string            `json:"model"`
		Status     string            `json:"status"`      // completed / failed / in_progress ...
		Output     []json.RawMessage `json:"output"`      // 输出条目数组
		OutputText string            `json:"output_text"` // 便捷字段:全部输出文本
		Usage      *responsesUsage   `json:"usage"`
		Error      *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("响应体不是合法 JSON: %w", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("上游响应标记失败: %s", resp.Error.Message)
	}
	res.ResponseID = resp.ID
	if res.Model == "" {
		res.Model = resp.Model
	}
	// 优先使用便捷字段 output_text,缺失时从输出条目中提取
	res.Output = resp.OutputText
	if res.Output == "" {
		res.Output = extractOutputText(resp.Output)
	}
	if len(resp.Output) > 0 {
		if m, err := marshalRaw(resp.Output); err == nil {
			res.OutputMessages = m
		}
	}
	if resp.Status != "" {
		res.FinishReasons = []string{finishFromStatus(resp.Status)}
	}
	if resp.Usage != nil {
		res.Usage = &Usage{InputTokens: resp.Usage.InputTokens, OutputTokens: resp.Usage.OutputTokens}
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("响应未包含有效内容,放弃解析")
	}
	return nil
}

// parseSSEResponse 解析 SSE 流式响应:累积各输出条目的文本增量
func (p *openaiResponsesParser) parseSSEResponse(res *Result, body []byte) error {
	res.Stream = true
	dec := &sseDecoder{}
	events := append(dec.feed(body), dec.finish()...)

	deltas := make(map[int]string) // 按 output_index 累积的文本增量
	var outputText string
	for _, ev := range events {
		if ev.data == "" {
			continue
		}
		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(ev.data), &base); err != nil {
			return fmt.Errorf("SSE 事件不是合法 JSON: %w", err)
		}
		switch base.Type {
		case "response.created":
			var e struct {
				Response struct {
					ID    string `json:"id"`
					Model string `json:"model"`
				} `json:"response"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				res.ResponseID = e.Response.ID
				if res.Model == "" {
					res.Model = e.Response.Model
				}
			}
		case "response.output_text.delta":
			var e struct {
				OutputIndex int    `json:"output_index"`
				Delta       string `json:"delta"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				deltas[e.OutputIndex] += e.Delta // 同一输出条目的增量顺序拼接
			}
		case "response.completed":
			var e struct {
				Response struct {
					Status     string          `json:"status"`
					OutputText string          `json:"output_text"`
					Usage      *responsesUsage `json:"usage"`
				} `json:"response"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				outputText = e.Response.OutputText
				if e.Response.Status != "" {
					res.FinishReasons = []string{finishFromStatus(e.Response.Status)}
				}
				if e.Response.Usage != nil {
					res.Usage = &Usage{InputTokens: e.Response.Usage.InputTokens, OutputTokens: e.Response.Usage.OutputTokens}
				}
			}
		case "response.failed":
			return errors.New("上游响应标记为 failed,放弃解析")
			// response.in_progress / response.output_item.done 等事件无需处理
		}
	}
	// 优先使用完成事件携带的 output_text,缺失时按 output_index 排序拼接增量
	res.Output = outputText
	if res.Output == "" && len(deltas) > 0 {
		idx := make([]int, 0, len(deltas))
		for i := range deltas {
			idx = append(idx, i)
		}
		sort.Ints(idx)
		parts := make([]string, 0, len(idx))
		for _, i := range idx {
			parts = append(parts, deltas[i])
		}
		res.Output = strings.Join(parts, "\n") // 多个输出条目以换行连接
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("流式响应未包含有效内容,放弃解析")
	}
	return nil
}

// extractOutputText 从输出条目数组中提取全部文本内容(仅处理 message 类型条目的 content 文本)
func extractOutputText(output []json.RawMessage) string {
	var parts []string
	for _, item := range output {
		var m struct {
			Type    string `json:"type"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal(item, &m) != nil {
			continue // 无法识别的条目:跳过
		}
		if m.Type == "message" {
			for _, c := range m.Content {
				if c.Text != "" {
					parts = append(parts, c.Text)
				}
			}
		}
	}
	return strings.Join(parts, "\n")
}

// finishFromStatus 把 Responses 状态映射为 finish_reason 风格的值
func finishFromStatus(s string) string {
	switch s {
	case "completed":
		return "stop"
	case "failed":
		return "error"
	default:
		return s // in_progress/cancelled 等原样保留
	}
}
