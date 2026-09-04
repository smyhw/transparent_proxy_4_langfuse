// transparent_proxy_4_langfuse 主程序:透明 LLM 代理。
// 职责:加载配置、装配转发层/队列/解析层/遥测层,启动 HTTP 服务与上报 worker,
// 监听系统信号执行优雅停机(顺序:关服务器 → 关队列 → 等 worker 排空 → 刷出遥测数据)。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/logging"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/proxy"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/queue"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/telemetry"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	// 命令行参数:配置文件路径
	configPath := flag.String("config", "config.yml", "配置文件路径(YAML 格式)")
	flag.Parse()

	// 加载并校验配置(默认值 → YAML 覆盖 → 校验)
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置加载失败:", err)
		os.Exit(1)
	}
	log := logging.Setup(cfg.Logging)
	log.Info("透明 LLM 代理启动",
		"监听", cfg.Server.Listen,
		"上游", cfg.Target.URL,
		"langfuse端点", cfg.Langfuse.Endpoint,
		"上报启用", cfg.LangfuseEnabled(),
	)

	// 组装各组件:队列 → 解析器注册表 → 转发处理器
	q := queue.NewQueue(cfg.Queue.Size)
	registry := parser.NewRegistry(cfg.Parser.Enabled)
	p, err := proxy.New(cfg, q, registry)
	if err != nil {
		log.Error("代理初始化失败", "error", err)
		os.Exit(1)
	}

	// 遥测出口:仅当配置了密钥时启用(否则纯转发,不解析不上报)
	var tp *sdktrace.TracerProvider
	if cfg.LangfuseEnabled() {
		tp, err = telemetry.NewTracerProvider(&cfg.Langfuse)
		if err != nil {
			log.Error("遥测初始化失败", "error", err)
			os.Exit(1)
		}
		defer tp.Shutdown(context.Background())
	} else {
		log.Warn("未配置 Langfuse 密钥:仅转发,不解析不上报")
	}

	// 异步解析与上报 worker 池
	worker := telemetry.NewWorker(q, registry, tp, cfg)
	worker.Start()

	// 启动 HTTP 服务(独立协程,错误经通道上报)
	server := proxy.NewServer(&cfg.Server, p)
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	log.Info("HTTP 服务已启动", "addr", cfg.Server.Listen)

	// 周期统计日志(转发/入队/丢弃/解析计数)
	stopStats := startStatsReporter(log, cfg, p, q, worker)
	defer stopStats()

	// 等待退出信号或服务异常
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("收到退出信号,开始优雅停机", "signal", s.String())
	case err := <-errCh:
		log.Error("HTTP 服务异常退出", "error", err)
	}

	// 停机顺序:
	// ① 停止接收新请求并等待在途请求完成(期间照常捕获入队)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warn("停机时存在未完成的请求", "error", err)
	}
	// ② 关闭队列:worker 排空剩余记录后自然退出
	q.Close()
	worker.Wait()
	// ③ 强制刷出批量队列中剩余的 span
	if tp != nil {
		flushCtx, cancel2 := context.WithTimeout(context.Background(), cfg.Langfuse.FlushTimeout.Duration())
		if err := tp.ForceFlush(flushCtx); err != nil {
			log.Warn("停机时遥测数据未完全刷出", "error", err)
		}
		cancel2()
	}

	log.Info("代理已退出", "转发统计", p.Stats(), "队列统计", q.Stats(), "worker统计", worker.Stats())
}

// startStatsReporter 启动周期统计日志协程,返回停止函数;间隔为 0 时不启动
func startStatsReporter(log *slog.Logger, cfg *config.Config, p *proxy.Proxy, q *queue.Queue, w *telemetry.Worker) func() {
	if cfg.Logging.StatsInterval <= 0 {
		return func() {}
	}
	stop := make(chan struct{})
	ticker := time.NewTicker(cfg.Logging.StatsInterval.Duration())
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Info("运行统计", "转发", p.Stats(), "队列", q.Stats(), "解析", w.Stats())
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(stop) }
}
