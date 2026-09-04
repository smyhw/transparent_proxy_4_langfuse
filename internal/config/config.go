// Package config 负责解析与校验 transparent_proxy_4_langfuse 的 YAML 配置文件。
// 所有配置项均有内置默认值:YAML 中缺失的字段保持默认值不变,详见 Default 函数与
// 根目录的 config.example.yml(两者注释一一对应)。
package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"gopkg.in/yaml.v3"
)

// Config 是顶层配置结构,对应 YAML 文件中的七个一级段落
type Config struct {
	Server   ServerConfig   `yaml:"server"`   // 代理服务器(监听端)配置
	Target   TargetConfig   `yaml:"target"`   // 上游目标(转发目的地)配置
	Capture  CaptureConfig  `yaml:"capture"`  // 流量捕获上限配置
	Queue    QueueConfig    `yaml:"queue"`    // 异步上报队列配置
	Parser   ParserConfig   `yaml:"parser"`   // 解析器配置
	Langfuse LangfuseConfig `yaml:"langfuse"` // Langfuse(OpenTelemetry)上报配置
	Logging  LoggingConfig  `yaml:"logging"`  // 日志配置
}

// ServerConfig 是代理服务器(监听端)配置
type ServerConfig struct {
	Listen            string   `yaml:"listen"`              // 监听地址,如 ":8080" 或 "127.0.0.1:8080"
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"` // 读取请求头超时,防慢速攻击
	IdleTimeout       Duration `yaml:"idle_timeout"`        // 空闲连接保活时间
	ShutdownTimeout   Duration `yaml:"shutdown_timeout"`    // 优雅停机等待在途请求完成的最长时间
}

// TargetConfig 是上游目标(转发目的地)配置
type TargetConfig struct {
	URL                 string `yaml:"url"`                     // 上游根地址,请求路径与查询串原样拼接
	PreserveHost        bool   `yaml:"preserve_host"`           // true=保留客户端 Host 头;false=改为上游主机
	SetXForwardedFor    bool   `yaml:"set_x_forwarded_for"`     // 是否追加 X-Forwarded-For(标准代理行为)
	MaxIdleConnsPerHost int    `yaml:"max_idle_conns_per_host"` // 到上游的保活连接数
}

// CaptureConfig 是流量捕获上限配置(只影响解析与上报,绝不影响转发)
type CaptureConfig struct {
	MaxRequestBytes  ByteSize `yaml:"max_request_bytes"`  // 请求体捕获上限,超出置截断标记
	MaxResponseBytes ByteSize `yaml:"max_response_bytes"` // 响应体捕获上限,超出置截断标记
}

// QueueConfig 是异步上报队列配置
type QueueConfig struct {
	Size       int    `yaml:"size"`        // 有界队列容量(条)
	Workers    int    `yaml:"workers"`     // 消费协程数
	FullPolicy string `yaml:"full_policy"` // 队列满策略:drop=丢弃并计数;block=等待空位
}

// ParserConfig 是解析器配置
type ParserConfig struct {
	Enabled         []string `yaml:"enabled"`          // 启用哪些解析器(openai_chat/anthropic/openai_responses)
	PathInclude     []string `yaml:"path_include"`     // 额外候选路径(后缀匹配)
	PathExclude     []string `yaml:"path_exclude"`     // 排除路径(即使命中解析器也仅转发)
	IncludeMessages bool     `yaml:"include_messages"` // 是否额外上报完整消息数组(体积大,默认关)
}

// LangfuseConfig 是 Langfuse(OpenTelemetry 原生接入)配置
type LangfuseConfig struct {
	Endpoint         string      `yaml:"endpoint"`            // Langfuse 根地址;自托管需 >= v3.22.0
	PublicKey        string      `yaml:"public_key"`          // 公钥(pk-lf-...);留空=完全关闭捕获与上报
	SecretKey        string      `yaml:"secret_key"`          // 密钥(sk-lf-...),注意文件权限
	IngestionVersion int         `yaml:"ingestion_version"`   // 请求头 x-langfuse-ingestion-version 的值
	ServiceName      string      `yaml:"service_name"`        // 资源属性 service.name
	Environment      string      `yaml:"environment"`         // 资源属性 deployment.environment
	UserHeader       string      `yaml:"user_header"`         // 提取 langfuse.user.id 的请求头名,空=不提取
	SessionHeader    string      `yaml:"session_header"`      // 提取会话 id 的请求头名,空=不提取
	UserKeyAsUserID  bool        `yaml:"user_key_as_user_id"` // 是否把客户端 API key 作为 langfuse.user.id 上报,默认关
	TraceName        string      `yaml:"trace_name"`          // langfuse.trace.name 静态覆盖,空=与 span 名一致
	Batch            BatchConfig `yaml:"batch"`               // 批量上报参数
	ExportTimeout    Duration    `yaml:"export_timeout"`      // 单次导出 HTTP 超时
	FlushTimeout     Duration    `yaml:"flush_timeout"`       // 停机排空批量队列的最大等待
}

// BatchConfig 是 OTel SDK 批量处理器参数
type BatchConfig struct {
	Timeout   Duration `yaml:"timeout"`    // 批量上报等待时间(延迟与吞吐的平衡)
	MaxSize   int      `yaml:"max_size"`   // 单批最大 span 数
	QueueSize int      `yaml:"queue_size"` // SDK 内部批量队列上限,超出由 SDK 丢弃(内存有界)
}

// LoggingConfig 是日志配置
type LoggingConfig struct {
	Level         string   `yaml:"level"`          // debug | info | warn | error
	StatsInterval Duration `yaml:"stats_interval"` // 周期输出统计的间隔,0=关闭
}

// Duration 是支持 "10s"、"2m" 等字符串的时长类型(内部为 time.Duration)
type Duration time.Duration

// Duration 返回标准库 time.Duration 值
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalYAML 解析时长字符串(如 "10s"),也兼容 yaml 数值(纳秒)
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err == nil {
		v, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("无效的时长 %q(示例: 10s、2m、1h): %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	// 允许直接写数字(按纳秒解析,与 time.Duration 语义一致)
	var n int64
	if err := node.Decode(&n); err != nil {
		return fmt.Errorf("时长必须是字符串(如 10s)或整数纳秒")
	}
	*d = Duration(n)
	return nil
}

// ByteSize 是支持 "4MiB"、"1MB"、"1024" 等后缀的字节数类型
type ByteSize int64

// UnmarshalYAML 解析字节数字符串(支持 B/KB/MB/GB 与 KiB/MiB/GiB 后缀,大小写不敏感)
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err == nil {
		n, err := parseByteSize(strings.TrimSpace(s))
		if err != nil {
			return err
		}
		*b = ByteSize(n)
		return nil
	}
	// 允许直接写纯整数(单位:字节)
	var n int64
	if err := node.Decode(&n); err != nil {
		return fmt.Errorf("字节数必须是字符串(如 4MiB)或纯整数")
	}
	*b = ByteSize(n)
	return nil
}

// parseByteSize 解析带后缀的字节数字符串;KiB 按 1024 进制,KB 按 1000 进制
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("字节数不能为空")
	}
	// 拆出末尾的字母后缀与前面的数字部分
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	numPart, unitPart := s[:i], strings.ToUpper(s[i:])
	if numPart == "" {
		return 0, fmt.Errorf("无效的字节数 %q", s)
	}
	f, err := strconv.ParseFloat(numPart, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("无效的字节数 %q", s)
	}
	var mult float64 = 1
	switch unitPart {
	case "", "B":
		mult = 1
	case "KB":
		mult = 1000
	case "MB":
		mult = 1000 * 1000
	case "GB":
		mult = 1000 * 1000 * 1000
	case "KIB":
		mult = 1024
	case "MIB":
		mult = 1024 * 1024
	case "GIB":
		mult = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("无效的字节数单位 %q(支持 B/KB/MB/GB/KiB/MiB/GiB)", s[i:])
	}
	return int64(f * mult), nil
}

// Default 返回内置默认值(与 config.example.yml 注释一一对应)
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Listen:            ":8080",
			ReadHeaderTimeout: Duration(10 * time.Second),
			IdleTimeout:       Duration(120 * time.Second),
			ShutdownTimeout:   Duration(30 * time.Second),
		},
		Target: TargetConfig{
			URL:                 "http://127.0.0.1:11434",
			PreserveHost:        false,
			SetXForwardedFor:    true,
			MaxIdleConnsPerHost: 100,
		},
		Capture: CaptureConfig{
			MaxRequestBytes:  4 << 20, // 4MiB
			MaxResponseBytes: 4 << 20, // 4MiB
		},
		Queue: QueueConfig{
			Size:       128,
			Workers:    2,
			FullPolicy: "drop",
		},
		Parser: ParserConfig{
			Enabled:         []string{"openai_chat", "anthropic", "openai_responses"},
			IncludeMessages: false,
		},
		Langfuse: LangfuseConfig{
			Endpoint:         "https://cloud.langfuse.com",
			IngestionVersion: 4,
			ServiceName:      "langfuse-proxy",
			Environment:      "production",
			UserKeyAsUserID:  false, // 默认关闭:不把客户端 API key 作为用户 id 上报
			Batch: BatchConfig{
				Timeout:   Duration(2 * time.Second),
				MaxSize:   512,
				QueueSize: 2048,
			},
			ExportTimeout: Duration(10 * time.Second),
			FlushTimeout:  Duration(10 * time.Second),
		},
		Logging: LoggingConfig{
			Level:         "info",
			StatsInterval: Duration(60 * time.Second),
		},
	}
}

// Validate 校验配置值域;返回描述全部问题的聚合错误
func (c *Config) Validate() error {
	var problems []string
	if c.Server.Listen == "" {
		problems = append(problems, "server.listen 不能为空")
	}
	if c.Server.ReadHeaderTimeout <= 0 || c.Server.IdleTimeout <= 0 || c.Server.ShutdownTimeout <= 0 {
		problems = append(problems, "server 各项超时必须大于 0")
	}
	// 目标地址必须是合法的 http/https URL
	if u, err := url.Parse(c.Target.URL); err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		problems = append(problems, "target.url 必须是合法的 http/https 地址")
	}
	if c.Target.MaxIdleConnsPerHost <= 0 {
		problems = append(problems, "target.max_idle_conns_per_host 必须大于 0")
	}
	if c.Capture.MaxRequestBytes <= 0 || c.Capture.MaxResponseBytes <= 0 {
		problems = append(problems, "capture 上限必须大于 0")
	}
	if c.Queue.Size <= 0 {
		problems = append(problems, "queue.size 必须大于 0")
	}
	if c.Queue.Workers <= 0 {
		problems = append(problems, "queue.workers 必须大于 0")
	}
	if c.Queue.FullPolicy != "drop" && c.Queue.FullPolicy != "block" {
		problems = append(problems, "queue.full_policy 必须是 drop 或 block")
	}
	// 解析器名称必须合法(名单以 parser 包为唯一来源)
	for _, name := range c.Parser.Enabled {
		if !parser.IsKnown(name) {
			problems = append(problems, fmt.Sprintf("parser.enabled 含未知解析器 %q", name))
		}
	}
	// Langfuse 公钥与密钥必须成对出现
	if (c.Langfuse.PublicKey == "") != (c.Langfuse.SecretKey == "") {
		problems = append(problems, "langfuse.public_key 与 langfuse.secret_key 必须同时配置")
	}
	if c.Langfuse.PublicKey != "" {
		if u, err := url.Parse(c.Langfuse.Endpoint); err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https") {
			problems = append(problems, "langfuse.endpoint 必须是合法的 http/https 地址")
		}
		if c.Langfuse.IngestionVersion < 1 {
			problems = append(problems, "langfuse.ingestion_version 必须大于 0")
		}
		if c.Langfuse.Batch.Timeout <= 0 || c.Langfuse.Batch.MaxSize <= 0 || c.Langfuse.Batch.QueueSize <= 0 {
			problems = append(problems, "langfuse.batch 各项必须大于 0")
		}
		if c.Langfuse.ExportTimeout <= 0 || c.Langfuse.FlushTimeout <= 0 {
			problems = append(problems, "langfuse 各项超时必须大于 0")
		}
	}
	switch strings.ToLower(c.Logging.Level) {
	case "debug", "info", "warn", "error":
	default:
		problems = append(problems, "logging.level 必须是 debug/info/warn/error 之一")
	}
	if len(problems) > 0 {
		return fmt.Errorf("配置存在 %d 个问题:\n  - %s", len(problems), strings.Join(problems, "\n  - "))
	}
	return nil
}

// LangfuseEnabled 报告是否已配置 Langfuse 密钥(决定是否启用捕获、解析与上报)
func (c *Config) LangfuseEnabled() bool {
	return c.Langfuse.PublicKey != "" && c.Langfuse.SecretKey != ""
}
