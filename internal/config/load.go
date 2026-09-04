// load.go 提供配置文件加载入口:读取 YAML 文件、填充默认值、执行校验。
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load 读取并解析 YAML 配置文件。
// 流程:先构造默认值,再在默认值之上覆盖 YAML 内容(未出现的字段保持默认),
// 最后执行值域校验。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %q 失败: %w", path, err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %q 失败: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	return cfg, nil
}
