// server.go 构建监听用 http.Server。
package proxy

import (
	"net/http"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
)

// NewServer 构建监听用 http.Server。
// 刻意不设置 ReadTimeout/WriteTimeout:这两个超时会切断 SSE 长流式响应;
// 仅设置 ReadHeaderTimeout(防慢速攻击)与 IdleTimeout(连接保活)。
func NewServer(cfg *config.ServerConfig, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.IdleTimeout.Duration(),
	}
}
