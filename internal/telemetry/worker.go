// worker.go 实现异步消费循环:从队列取出流量快照 → 匹配解析器 → 解析 →
// 构建并上报 span。全部工作发生在独立协程中,与转发路径完全解耦。
package telemetry

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/queue"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// WorkerStats 是 worker 池的统计快照(供周期日志输出)
type WorkerStats struct {
	Matched uint64 // 命中解析器的记录数
	Parsed  uint64 // 解析成功数
	Failed  uint64 // 解析失败数(降级:不上报)
}

// Worker 是异步解析与上报的消费端
type Worker struct {
	queue           *queue.Queue             // 流量快照来源
	registry        *parser.Registry         // 解析器注册表
	tp              *sdktrace.TracerProvider // 遥测出口;nil=未启用 langfuse(只计数)
	langfuseCfg     *config.LangfuseConfig   // 上报相关配置
	includeMessages bool                     // 是否上报完整消息数组
	workers         int                      // 消费协程数
	matched         atomic.Uint64            // 命中解析器的记录数
	parsed          atomic.Uint64            // 解析成功数
	failed          atomic.Uint64            // 解析失败数
	wg              sync.WaitGroup           // 等待全部消费协程退出
	log             *slog.Logger             // 结构化日志
}

// NewWorker 创建 worker 池;tp 为 nil 时仅统计不实际上报
func NewWorker(q *queue.Queue, reg *parser.Registry, tp *sdktrace.TracerProvider, cfg *config.Config) *Worker {
	return &Worker{
		queue:           q,
		registry:        reg,
		tp:              tp,
		langfuseCfg:     &cfg.Langfuse,
		includeMessages: cfg.Parser.IncludeMessages,
		workers:         cfg.Queue.Workers,
		log:             slog.Default(),
	}
}

// Start 启动 workers 个消费协程;队列 Close 后协程排空剩余记录自然退出
func (w *Worker) Start() {
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.run()
		}()
	}
}

// run 是单个消费协程的主循环:for-range 消费至通道关闭
func (w *Worker) run() {
	for rec := range w.queue.Ch() {
		w.process(rec)
		w.queue.AddConsumed(1)
	}
}

// process 处理一条流量快照:匹配 → 解析 → 上报;任何失败都只计数不影响转发
func (w *Worker) process(rec *record.Record) {
	p := w.registry.Match(rec)
	if p == nil {
		return // 未命中任何解析器:无需处理
	}
	w.matched.Add(1)
	res, err := p.Parse(rec)
	if err != nil {
		// 解析失败是明确的降级语义:不上报残缺数据,仅 debug 日志(不打 body)
		w.failed.Add(1)
		w.log.Debug("流量解析失败(仅影响上报,不影响转发)", "parser", p.Name(), "path", rec.URLPath, "error", err)
		return
	}
	w.parsed.Add(1)
	if w.tp != nil {
		buildSpan(rec, res, w.langfuseCfg, w.includeMessages, w.tp)
	}
}

// Wait 等待全部消费协程退出(配合队列 Close 使用)
func (w *Worker) Wait() { w.wg.Wait() }

// Stats 返回统计快照
func (w *Worker) Stats() WorkerStats {
	return WorkerStats{
		Matched: w.matched.Load(),
		Parsed:  w.parsed.Load(),
		Failed:  w.failed.Load(),
	}
}
