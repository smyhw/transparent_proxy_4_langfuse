// anthropic.go 实现 Anthropic Messages 协议解析器(/v1/messages)。
// 兼容 JSON 与 SSE(流式)两种响应;SSE 按事件机分发处理
// (message_start / content_block_delta / message_delta 等)。
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

// anthropicParser 是 Anthropic Messages 协议解析器
type anthropicParser struct{}

// Name 返回解析器名称
func (p *anthropicParser) Name() string { return "anthropic" }

// PathSuffixes 返回该解析器覆盖的路径后缀
func (p *anthropicParser) PathSuffixes() []string { return []string{"/v1/messages"} }

// Match 依据方法、路径后缀与 Content-Type 判定(header 级,极快)
func (p *anthropicParser) Match(r *record.Record) bool {
	return r.Method == http.MethodPost &&
		strings.HasSuffix(r.URLPath, "/v1/messages") &&
		AcceptsJSON(r.RequestHeaders)
}

// anthropicRequest 是 Messages 请求体的解析子集
type anthropicRequest struct {
	Model       string            `json:"model"`
	MaxTokens   *int64            `json:"max_tokens"`
	Messages    []json.RawMessage `json:"messages"`
	System      json.RawMessage   `json:"system"` // 字符串或文本块数组
	Stream      bool              `json:"stream"`
	Temperature *float64          `json:"temperature"`
	TopP        *float64          `json:"top_p"`
	Metadata    struct {
		UserID string `json:"user_id"` // Anthropic 请求体 metadata 中的用户标识
	} `json:"metadata"`
}

// anthropicUsage 是 Messages 的 usage 结构
type anthropicUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// anthropicBlock 是响应内容块的内部累积结构(按块 index 关联)
type anthropicBlock struct {
	index int    // 块序号
	kind  string // 块类型:text / tool_use / thinking 等
	text  string // 文本内容(text_delta)或工具入参 JSON(input_json_delta)
	name  string // tool_use 块的名称
	id    string // tool_use 块的 id
}

// Parse 解析请求与响应,产出归一化结果;失败返回 error(调用方不上报)
func (p *anthropicParser) Parse(r *record.Record) (*Result, error) {
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
	var req anthropicRequest
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return nil, fmt.Errorf("请求体不是合法 JSON: %w", err)
	}
	res := &Result{
		Provider:    "anthropic",
		Operation:   "chat",
		Model:       req.Model,
		Stream:      req.Stream,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
		TopP:        req.TopP,
		UserID:      req.Metadata.UserID,
	}
	// 输入内容:重新序列化 messages 数组作为 gen_ai.prompt
	if len(req.Messages) > 0 {
		if in, err := marshalRaw(req.Messages); err == nil {
			res.Input = in
			res.InputMessages = in
		}
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
func (p *anthropicParser) parseResponse(res *Result, body []byte) error {
	if looksLikeJSON(body) {
		return p.parseJSONResponse(res, body)
	}
	return p.parseSSEResponse(res, body)
}

// parseJSONResponse 解析一次性(非流式)JSON 响应
func (p *anthropicParser) parseJSONResponse(res *Result, body []byte) error {
	var resp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string          `json:"stop_reason"`
		Usage      *anthropicUsage `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("响应体不是合法 JSON: %w", err)
	}
	res.ResponseID = resp.ID
	if res.Model == "" {
		res.Model = resp.Model
	}
	var outParts []string
	for _, c := range resp.Content {
		// 仅累积 text 块;thinking/signature 等推理内容刻意忽略
		if c.Type == "text" && c.Text != "" {
			outParts = append(outParts, c.Text)
		}
	}
	res.Output = strings.Join(outParts, "\n")
	// 结构化输出:完整 content 数组原样保留(含 tool_use 等块)
	if m, err := json.Marshal(resp.Content); err == nil {
		res.OutputMessages = string(m)
	}
	if resp.StopReason != "" {
		res.FinishReasons = []string{resp.StopReason}
	}
	if resp.Usage != nil {
		res.Usage = &Usage{InputTokens: resp.Usage.InputTokens, OutputTokens: resp.Usage.OutputTokens}
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("响应未包含有效内容,放弃解析")
	}
	return nil
}

// parseSSEResponse 解析 SSE 流式响应:按事件类型分发累积
func (p *anthropicParser) parseSSEResponse(res *Result, body []byte) error {
	res.Stream = true
	dec := &sseDecoder{}
	events := append(dec.feed(body), dec.finish()...)

	blocks := make(map[int]*anthropicBlock) // 按 index 关联的内容块
	var inputTokens, outputTokens int64
	var hasInputUsage, hasOutputUsage bool
	var stopReason string
	for _, ev := range events {
		if ev.data == "" {
			continue
		}
		// 先解出事件类型,再按类型解出具体载荷
		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(ev.data), &base); err != nil {
			return fmt.Errorf("SSE 事件不是合法 JSON: %w", err)
		}
		switch base.Type {
		case "message_start":
			var e struct {
				Message struct {
					ID    string          `json:"id"`
					Model string          `json:"model"`
					Usage *anthropicUsage `json:"usage"`
				} `json:"message"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				res.ResponseID = e.Message.ID
				if res.Model == "" {
					res.Model = e.Message.Model
				}
				if e.Message.Usage != nil {
					inputTokens = e.Message.Usage.InputTokens
					hasInputUsage = true
				}
			}
		case "content_block_start":
			var e struct {
				Index        int `json:"index"`
				ContentBlock struct {
					Type string `json:"type"`
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"content_block"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				blocks[e.Index] = &anthropicBlock{index: e.Index, kind: e.ContentBlock.Type, id: e.ContentBlock.ID, name: e.ContentBlock.Name}
			}
		case "content_block_delta":
			var e struct {
				Index int `json:"index"`
				Delta struct {
					Type        string `json:"type"`
					Text        string `json:"text"`
					PartialJSON string `json:"partial_json"`
				} `json:"delta"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				b := blocks[e.Index]
				if b == nil {
					b = &anthropicBlock{index: e.Index}
					blocks[e.Index] = b
				}
				switch e.Delta.Type {
				case "text_delta":
					b.text += e.Delta.Text // 文本增量拼接
				case "input_json_delta":
					b.text += e.Delta.PartialJSON // 工具入参 JSON 分片拼接
				default:
					// thinking_delta/signature_delta 等推理增量刻意忽略
				}
			}
		case "message_delta":
			var e struct {
				Delta struct {
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
				Usage *anthropicUsage `json:"usage"`
			}
			if err := json.Unmarshal([]byte(ev.data), &e); err == nil {
				stopReason = e.Delta.StopReason
				if e.Usage != nil {
					outputTokens = e.Usage.OutputTokens
					hasOutputUsage = true
				}
			}
		case "error":
			return errors.New("上游响应包含 error 事件,放弃解析")
			// message_stop / content_block_stop 等其余事件无需处理
		}
	}
	if stopReason != "" {
		res.FinishReasons = []string{stopReason}
	}
	if hasInputUsage || hasOutputUsage {
		res.Usage = &Usage{InputTokens: inputTokens, OutputTokens: outputTokens}
	}
	// 汇总内容块:按 index 排序,text 块拼接为输出,其余块进入结构化输出
	list := make([]*anthropicBlock, 0, len(blocks))
	for _, b := range blocks {
		list = append(list, b)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].index < list[j].index })
	var outParts []string
	var structured []map[string]any
	for _, b := range list {
		switch b.kind {
		case "text":
			if b.text != "" {
				outParts = append(outParts, b.text)
			}
			structured = append(structured, map[string]any{"type": "text", "text": b.text})
		case "tool_use":
			structured = append(structured, map[string]any{"type": "tool_use", "id": b.id, "name": b.name, "input": json.RawMessage(b.text)})
		default:
			// thinking 等块不进入输出
		}
	}
	res.Output = strings.Join(outParts, "\n")
	if len(structured) > 0 {
		if m, err := json.Marshal(structured); err == nil {
			res.OutputMessages = string(m)
		}
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("流式响应未包含有效内容,放弃解析")
	}
	return nil
}
