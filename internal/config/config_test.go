// config_test.go 测试配置加载、默认值填充、值域校验与 ByteSize/Duration 解析。
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestDefault 校验内置默认值的关键字段
func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg.Server.Listen != ":8080" {
		t.Errorf("默认监听地址 = %q,期望 :8080", cfg.Server.Listen)
	}
	if cfg.Target.URL != "http://127.0.0.1:11434" {
		t.Errorf("默认上游 = %q", cfg.Target.URL)
	}
	if cfg.Capture.MaxRequestBytes != 4<<20 || cfg.Capture.MaxResponseBytes != 4<<20 {
		t.Errorf("默认捕获上限应为 4MiB")
	}
	if cfg.Queue.Size != 128 || cfg.Queue.Workers != 2 || cfg.Queue.FullPolicy != "drop" {
		t.Errorf("默认队列配置不符: %+v", cfg.Queue)
	}
	if len(cfg.Parser.Enabled) != 3 {
		t.Errorf("默认应启用 3 个解析器,实际 %d", len(cfg.Parser.Enabled))
	}
	if cfg.Langfuse.IngestionVersion != 4 || cfg.Langfuse.Endpoint != "https://cloud.langfuse.com" {
		t.Errorf("默认 Langfuse 配置不符")
	}
	if cfg.LangfuseEnabled() {
		t.Errorf("默认不应启用 langfuse(密钥为空)")
	}
	if cfg.Langfuse.UserKeyAsUserID {
		t.Errorf("默认不应开启 user_key_as_user_id")
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("默认配置应通过校验: %v", err)
	}
}

// TestLoadOverrides 验证 YAML 覆盖与未覆盖字段保持默认
func TestLoadOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.yml")
	content := `
server:
  listen: "127.0.0.1:9090"
  read_header_timeout: 5s
target:
  url: "http://example.com:8080"
queue:
  full_policy: block
  workers: 4
langfuse:
  endpoint: "http://localhost:3000"
  public_key: "pk-test"
  secret_key: "sk-test"
  user_key_as_user_id: true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Server.Listen != "127.0.0.1:9090" {
		t.Errorf("listen 未被覆盖: %q", cfg.Server.Listen)
	}
	if cfg.Server.ReadHeaderTimeout.Duration() != 5*time.Second {
		t.Errorf("read_header_timeout 解析错误: %v", cfg.Server.ReadHeaderTimeout)
	}
	if cfg.Target.URL != "http://example.com:8080" {
		t.Errorf("target.url 未被覆盖")
	}
	if cfg.Queue.FullPolicy != "block" || cfg.Queue.Workers != 4 {
		t.Errorf("queue 未被覆盖: %+v", cfg.Queue)
	}
	// 未覆盖字段保持默认
	if cfg.Queue.Size != 128 {
		t.Errorf("queue.size 应保持默认 128,实际 %d", cfg.Queue.Size)
	}
	if cfg.Capture.MaxRequestBytes != 4<<20 {
		t.Errorf("capture 应保持默认")
	}
	if cfg.Langfuse.IngestionVersion != 4 {
		t.Errorf("ingestion_version 应保持默认 4")
	}
	if !cfg.LangfuseEnabled() {
		t.Errorf("配置了密钥后应启用 langfuse")
	}
	if !cfg.Langfuse.UserKeyAsUserID {
		t.Errorf("user_key_as_user_id 未被覆盖")
	}
}

// TestLoadInvalid 验证非法配置被校验拦截
func TestLoadInvalid(t *testing.T) {
	cases := map[string]string{
		"非法队列策略": "queue:\n  full_policy: foo\n",
		"未知解析器":  "parser:\n  enabled: [not_exist]\n",
		"密钥不成对":  "langfuse:\n  public_key: pk-only\n",
		"非法端点":   "langfuse:\n  endpoint: \"://bad\"\n  public_key: pk\n  secret_key: sk\n",
		"非法日志级别": "logging:\n  level: verbose\n",
		"非法字节数":  "capture:\n  max_request_bytes: 12XB\n",
		"非法时长":   "server:\n  read_header_timeout: 快\n",
		"非法上游协议": "target:\n  url: \"ftp://x.com\"\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.yml")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Errorf("期望校验失败,实际通过")
			}
		})
	}
}

// TestByteSize 验证字节数字符串解析
func TestByteSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"4MiB", 4 << 20},
		{"4MIB", 4 << 20},
		{"1MB", 1000 * 1000},
		{"1024", 1024},
		{"2GiB", 2 << 30},
		{"512KB", 512 * 1000},
		{"10B", 10},
	}
	for _, c := range cases {
		var b ByteSize
		if err := b.UnmarshalYAML(mustYAMLNode(t, c.in)); err != nil {
			t.Errorf("%q 解析失败: %v", c.in, err)
			continue
		}
		if int64(b) != c.want {
			t.Errorf("%q = %d,期望 %d", c.in, int64(b), c.want)
		}
	}
	for _, bad := range []string{"", "abc", "12XB", "-5MiB"} {
		var b ByteSize
		if err := b.UnmarshalYAML(mustYAMLNode(t, bad)); err == nil {
			t.Errorf("%q 应解析失败", bad)
		}
	}
}

// TestDuration 验证时长字符串解析
func TestDuration(t *testing.T) {
	var d Duration
	if err := d.UnmarshalYAML(mustYAMLNode(t, "10s")); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if d.Duration() != 10*time.Second {
		t.Errorf("10s = %v", d.Duration())
	}
	if err := d.UnmarshalYAML(mustYAMLNode(t, "2m30s")); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if d.Duration() != 150*time.Second {
		t.Errorf("2m30s = %v", d.Duration())
	}
	if err := d.UnmarshalYAML(mustYAMLNode(t, "不好")); err == nil {
		t.Errorf("非法时长应解析失败")
	}
}

// mustYAMLNode 构造一个标量 yaml.Node,供 UnmarshalYAML 直接调用
func mustYAMLNode(t *testing.T, s string) *yaml.Node {
	t.Helper()
	return &yaml.Node{Kind: yaml.ScalarNode, Value: s}
}
