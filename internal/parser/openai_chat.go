// openai_chat.go 实现 OpenAI Chat Completions 协议解析器(/v1/chat/completions)。
// 兼容 JSON 与 SSE(流式)两种响应;对 OpenAI 兼容服务(DeepSeek、Moonshot、
// Ollama 等)同样适用。SSE 响应按 chunk 重建完整文本与工具调用。
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

// openaiChatParser 是 OpenAI Chat Completions 协议解析器
type openaiChatParser struct{}

// Name 返回解析器名称
func (p *openaiChatParser) Name() string { return "openai_chat" }

// PathSuffixes 返回该解析器覆盖的路径后缀(兼容 Azure 等带前缀的部署路径)
func (p *openaiChatParser) PathSuffixes() []string { return []string{"/chat/completions"} }

// Match 依据方法、路径后缀与 Content-Type 判定(header 级,极快)
func (p *openaiChatParser) Match(r *record.Record) bool {
	return r.Method == http.MethodPost &&
		strings.HasSuffix(r.URLPath, "/chat/completions") &&
		AcceptsJSON(r.RequestHeaders)
}

// chatRequest 是 Chat Completions 请求体的解析子集
type chatRequest struct {
	Model               string            `json:"model"`
	Messages            []json.RawMessage `json:"messages"`
	Stream              bool              `json:"stream"`
	Temperature         *float64          `json:"temperature"`
	MaxTokens           *int64            `json:"max_tokens"`            // 旧版字段
	MaxCompletionTokens *int64            `json:"max_completion_tokens"` // 新版字段
	TopP                *float64          `json:"top_p"`
	User                string            `json:"user"` // OpenAI 请求体自带 user 字段(可选用户标识)
}

// chatUsage 是 Chat Completions 的 usage 结构
type chatUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// chatToolCall 是流式分片中合并后的工具调用
type chatToolCall struct {
	Index     int    `json:"index"`
	ID        string `json:"id,omitempty"`
	Type      string `json:"type,omitempty"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Parse 解析请求与响应,产出归一化结果;失败返回 error(调用方不上报)
func (p *openaiChatParser) Parse(r *record.Record) (*Result, error) {
	if err := rejectTruncated(r); err != nil {
		return nil, err
	}
	// 解压(若 Content-Encoding: gzip;多数情况下 Transport 已透明解压)
	reqBody, err := decompress(r.RequestBody, r.RequestHeaders)
	if err != nil {
		return nil, fmt.Errorf("请求体解压失败: %w", err)
	}
	respBody, err := decompress(r.ResponseBody, r.ResponseHeaders)
	if err != nil {
		return nil, fmt.Errorf("响应体解压失败: %w", err)
	}

	// 解析请求体
	var req chatRequest
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
		UserID:      req.User,
	}
	// max_tokens 与 max_completion_tokens 并存时优先取后者(新版字段)
	if req.MaxCompletionTokens != nil {
		res.MaxTokens = req.MaxCompletionTokens
	} else {
		res.MaxTokens = req.MaxTokens
	}
	// 输入内容:重新序列化 messages 数组作为 gen_ai.prompt
	if len(req.Messages) > 0 {
		if in, err := marshalRaw(req.Messages); err == nil {
			res.Input = in
			res.InputMessages = in
		}
	}

	// 解析响应体(JSON 或 SSE 由首字节判定)
	if err := p.parseResponse(res, respBody); err != nil {
		return nil, err
	}
	if res.Model == "" {
		return nil, errors.New("缺少模型名,放弃解析")
	}
	return res, nil
}

// parseResponse 按响应体形态分发到 JSON 或 SSE 解析
func (p *openaiChatParser) parseResponse(res *Result, body []byte) error {
	if looksLikeJSON(body) {
		return p.parseJSONResponse(res, body)
	}
	return p.parseSSEResponse(res, body)
}

// parseJSONResponse 解析一次性(非流式)JSON 响应
func (p *openaiChatParser) parseJSONResponse(res *Result, body []byte) error {
	var resp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role      string            `json:"role"`
				Content   json.RawMessage   `json:"content"`
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *chatUsage `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("响应体不是合法 JSON: %w", err)
	}
	res.ResponseID = resp.ID
	if res.Model == "" {
		res.Model = resp.Model
	}
	var outParts []string
	var msgs []json.RawMessage
	for _, c := range resp.Choices {
		if c.Message.Content != nil {
			if text := stringifyContent(c.Message.Content); text != "" {
				outParts = append(outParts, text)
			}
		}
		if c.FinishReason != "" {
			res.FinishReasons = append(res.FinishReasons, c.FinishReason)
		}
		// 收集完整 assistant 消息(含工具调用)作为结构化输出
		if msg, err := json.Marshal(c.Message); err == nil {
			msgs = append(msgs, msg)
		}
	}
	res.Output = strings.Join(outParts, "\n") // 多 choice 时以换行分隔
	if len(msgs) > 0 {
		if m, err := marshalRaw(msgs); err == nil {
			res.OutputMessages = m
		}
	}
	if resp.Usage != nil {
		res.Usage = &Usage{InputTokens: resp.Usage.PromptTokens, OutputTokens: resp.Usage.CompletionTokens}
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("响应未包含有效内容,放弃解析")
	}
	return nil
}

// parseSSEResponse 解析 SSE 流式响应:按 chunk 重建文本、合并工具调用
func (p *openaiChatParser) parseSSEResponse(res *Result, body []byte) error {
	res.Stream = true // 响应为流式:即使请求未标 stream,也按流式处理
	dec := &sseDecoder{}
	events := append(dec.feed(body), dec.finish()...)

	var outParts []string                // 累积的文本增量
	tools := make(map[int]*chatToolCall) // 按 index 合并的工具调用
	var id, model string
	var usage *chatUsage
	var finish string
	for _, ev := range events {
		if ev.data == "[DONE]" {
			break // OpenAI 流结束哨兵
		}
		var chunk struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Role      string          `json:"role"`
					Content   json.RawMessage `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage *chatUsage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(ev.data), &chunk); err != nil {
			return fmt.Errorf("SSE 分块不是合法 JSON: %w", err)
		}
		if chunk.ID != "" {
			id = chunk.ID
		}
		if chunk.Model != "" {
			model = chunk.Model
		}
		if chunk.Usage != nil {
			usage = chunk.Usage // 仅当请求开启 stream_options.include_usage 时出现
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != nil {
				if text := stringifyContent(c.Delta.Content); text != "" {
					outParts = append(outParts, text)
				}
			}
			for _, tc := range c.Delta.ToolCalls {
				t := tools[tc.Index]
				if t == nil {
					t = &chatToolCall{Index: tc.Index}
					tools[tc.Index] = t
				}
				// 工具调用参数按分片顺序拼接
				if tc.ID != "" {
					t.ID = tc.ID
				}
				if tc.Type != "" {
					t.Type = tc.Type
				}
				if tc.Function.Name != "" {
					t.Name = tc.Function.Name
				}
				t.Arguments += tc.Function.Arguments
			}
			if c.FinishReason != "" {
				finish = c.FinishReason
			}
		}
	}
	res.ResponseID = id
	if res.Model == "" {
		res.Model = model
	}
	if finish != "" {
		res.FinishReasons = []string{finish}
	}
	if usage != nil {
		res.Usage = &Usage{InputTokens: usage.PromptTokens, OutputTokens: usage.CompletionTokens}
	}
	res.Output = strings.Join(outParts, "")
	// 工具调用:按 index 排序后序列化,作为结构化输出
	if len(tools) > 0 {
		list := make([]chatToolCall, 0, len(tools))
		for _, t := range tools {
			list = append(list, *t)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Index < list[j].Index })
		if tj, err := json.Marshal(list); err == nil {
			if res.Output != "" {
				res.Output += "\n"
			}
			res.Output += string(tj) // 纯工具调用回合:JSON 作为输出内容
		}
		// 结构化输出:assistant 消息(文本 + 工具调用)
		msg := map[string]any{"role": "assistant", "content": strings.Join(outParts, ""), "tool_calls": list}
		if mj, err := json.Marshal(msg); err == nil {
			res.OutputMessages = string(mj)
		}
	} else if res.Output != "" {
		msg := map[string]any{"role": "assistant", "content": res.Output}
		if mj, err := json.Marshal(msg); err == nil {
			res.OutputMessages = string(mj)
		}
	}
	if res.Output == "" && res.Usage == nil {
		return errors.New("流式响应未包含有效内容,放弃解析")
	}
	return nil
}
