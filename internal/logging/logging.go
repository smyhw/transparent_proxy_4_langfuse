// Package logging 负责初始化标准库结构化日志(slog)并集中日志脱敏约定。
// 约定:日志中禁止输出请求/响应头值与 body;密钥类敏感值仅输出掩码后的形式。
package logging

import (
	"log/slog"
	"os"
	"strings"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
)

// Setup 按配置初始化全局 slog logger 并返回
func Setup(cfg config.LoggingConfig) *slog.Logger {
	// 级别映射:非法值一律回落到 info
	var lvl slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(logger)
	return logger
}

// Mask 对敏感值做掩码:保留首尾各 4 个字符,中间以 *** 代替;过短的值全部掩掉
func Mask(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "***" + s[len(s)-4:]
}
