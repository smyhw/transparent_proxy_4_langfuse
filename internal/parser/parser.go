// Package parser 将捕获的 HTTP 流量解析为归一化的 LLM 调用结果,供遥测层构建 OTel span。
// 设计要点:
//   - Match 只看方法/路径/Content-Type 等 header 信息,极快,代理层据此判定候选请求;
//   - Parse 处理 body(解压/JSON/SSE 重建),较慢,只在 worker 协程中执行;
//   - 解析失败返回 error,调用方不上报 span(降级语义,绝不影响转发)。
package parser

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// Result 是解析成功后的归一化中间表示,遥测层据此构建 span
type Result struct {
	Provider       string   // 供应商标识("openai"/"anthropic")→ gen_ai.system
	Operation      string   // 操作类型,固定为 "chat"
	Model          string   // 模型名
	Stream         bool     // 是否为流式请求/响应
	Input          string   // 输入内容 → gen_ai.prompt(请求侧消息数组的 JSON 文本)
	Output         string   // 输出内容 → gen_ai.completion(响应文本,工具调用以 JSON 附后)
	InputMessages  string   // 完整输入消息数组 JSON(仅在 include_messages 开启时上报)
	OutputMessages string   // 完整输出消息 JSON(同上)
	Temperature    *float64 // 采样温度(尽力提取)
	MaxTokens      *int64   // 最大输出 token 数(尽力提取)
	TopP           *float64 // top_p(尽力提取)
	FinishReasons  []string // 结束原因(对应每个生成结果)
	ResponseID     string   // 响应 id
	Usage          *Usage   // token 用量;nil=缺失,不上报(不做估算,避免污染成本统计)
	SessionID      string   // 会话标识(gen_ai.conversation.id,由遥测层从请求头补充)
	UserID         string   // 用户标识(由遥测层从请求头/请求体 metadata 提取)
}

// Usage 是 token 用量
type Usage struct {
	InputTokens  int64 // 输入 token 数
	OutputTokens int64 // 输出 token 数
}

// Parser 是单个协议解析器的契约
type Parser interface {
	Name() string                            // 解析器名称(用于日志与统计)
	PathSuffixes() []string                  // 该解析器覆盖的路径后缀(供代理层候选判定)
	Match(r *record.Record) bool             // header 级判定(方法+路径+Content-Type),极快
	Parse(r *record.Record) (*Result, error) // body 级解析(慢路径,仅 worker 调用)
}

// Registry 是解析器注册表,按注册顺序匹配
type Registry struct {
	parsers []Parser
}

// NewRegistry 按 enabled 名单创建解析器注册表(注册顺序即匹配优先级)
func NewRegistry(enabled []string) *Registry {
	// 全部可用解析器的唯一来源;config 包的校验也依赖 IsKnown,保持同步
	all := map[string]Parser{
		(&openaiChatParser{}).Name():      &openaiChatParser{},
		(&anthropicParser{}).Name():       &anthropicParser{},
		(&openaiResponsesParser{}).Name(): &openaiResponsesParser{},
	}
	rg := &Registry{}
	for _, name := range enabled {
		if p, ok := all[name]; ok {
			rg.parsers = append(rg.parsers, p)
		}
	}
	return rg
}

// IsKnown 报告名称是否为已注册的解析器(供配置校验使用)
func IsKnown(name string) bool {
	switch name {
	case "openai_chat", "anthropic", "openai_responses":
		return true
	}
	return false
}

// Match 返回首个命中的解析器;无命中返回 nil(调用方不解析)
func (rg *Registry) Match(r *record.Record) Parser {
	for _, p := range rg.parsers {
		if p.Match(r) {
			return p
		}
	}
	return nil
}

// PathSuffixes 汇总所有已注册解析器的路径后缀(供代理层做候选判定)
func (rg *Registry) PathSuffixes() []string {
	var out []string
	for _, p := range rg.parsers {
		out = append(out, p.PathSuffixes()...)
	}
	return out
}

// AcceptsJSON 判断请求 Content-Type 是否允许 JSON 解析(缺失时宽松放行)
func AcceptsJSON(h http.Header) bool {
	ct := h.Get("Content-Type")
	return ct == "" || strings.Contains(strings.ToLower(ct), "application/json")
}

// maxDecompressedBytes 是解压后数据的最大字节数(防 zip bomb)
const maxDecompressedBytes = 32 << 20 // 32MiB

// decompress 按 Content-Encoding 解压;未压缩时原样返回
func decompress(data []byte, h http.Header) ([]byte, error) {
	if !strings.Contains(strings.ToLower(h.Get("Content-Encoding")), "gzip") {
		return data, nil // 未压缩:直接返回原字节
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip 解压失败: %w", err)
	}
	defer zr.Close()
	// 限制解压后大小,防止恶意压缩包耗尽内存
	out, err := io.ReadAll(io.LimitReader(zr, maxDecompressedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("gzip 解压失败: %w", err)
	}
	if len(out) > maxDecompressedBytes {
		return nil, errors.New("解压后数据超过大小上限")
	}
	return out, nil
}

// looksLikeJSON 判断响应体是否为 JSON(而非 SSE):依据首个非空白字节
func looksLikeJSON(body []byte) bool {
	trimmed := bytes.TrimLeft(body, " \t\r\n\ufeff")
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// stringifyContent 将可能为字符串或数组的 content 字段归一化为文本
func stringifyContent(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw) // 非字符串(如多模态内容数组):直接保留其 JSON 原文
}

// marshalRaw 将原始 JSON 片段数组序列化为 JSON 数组文本
func marshalRaw(items []json.RawMessage) (string, error) {
	out, err := json.Marshal(items)
	return string(out), err
}

// rejectTruncated 检查截断标记:截断的流量解析大概率失败,直接降级不上报
func rejectTruncated(r *record.Record) error {
	if r.RequestTruncated || r.ResponseTruncated {
		return errors.New("流量已截断,放弃解析(降级不上报)")
	}
	return nil
}
