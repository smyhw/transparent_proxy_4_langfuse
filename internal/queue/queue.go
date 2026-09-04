// Package queue 提供"转发完成 → 异步上报"之间的有界缓冲队列。
// 采用有界 channel:drop 策略用非阻塞投递保证转发协程零等待;
// block 策略在队列满时阻塞转发(用户显式选择的折中,需在配置中开启)。
package queue

import (
	"sync/atomic"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// Queue 是有界的上报队列,线程安全
type Queue struct {
	ch       chan *record.Record // 底层有界 channel
	closed   atomic.Bool         // 是否已关闭(关闭后再投递一律丢弃,防止 panic)
	enqueued atomic.Uint64       // 成功入队总数
	dropped  atomic.Uint64       // 因队列满/已关闭而丢弃的总数
	consumed atomic.Uint64       // 已被 worker 消费的总数
}

// QueueStats 是队列的统计快照(供周期日志输出)
type QueueStats struct {
	Enqueued uint64 // 成功入队总数
	Dropped  uint64 // 丢弃总数
	Consumed uint64 // 消费总数
}

// NewQueue 创建容量为 size 的队列。
// 投递策略由调用方选择:TryEnqueue 对应 drop,Enqueue 对应 block。
func NewQueue(size int) *Queue {
	return &Queue{ch: make(chan *record.Record, size)}
}

// TryEnqueue 以 drop 语义投递:队列满时立即返回 false 并计数,永不阻塞调用方(转发协程)
func (q *Queue) TryEnqueue(r *record.Record) bool {
	if q.closed.Load() {
		q.dropped.Add(1)
		return false
	}
	select {
	case q.ch <- r:
		q.enqueued.Add(1)
		return true
	default:
		q.dropped.Add(1) // 队列满:丢弃并计数(降级语义,转发不受影响)
		return false
	}
}

// Enqueue 以 block 语义投递:队列满时阻塞直到有空位。
// 注意:此路径会阻塞转发协程,仅当用户显式配置 full_policy=block 时使用。
func (q *Queue) Enqueue(r *record.Record) {
	if q.closed.Load() {
		q.dropped.Add(1)
		return
	}
	q.ch <- r
	q.enqueued.Add(1)
}

// Ch 返回消费端通道;Close 后 worker 用 for-range 自然排空剩余记录
func (q *Queue) Ch() <-chan *record.Record { return q.ch }

// Close 关闭队列:仅关闭投递端,已入队的记录仍可被排空消费
func (q *Queue) Close() {
	if !q.closed.CompareAndSwap(false, true) {
		return // 防重复关闭导致 panic
	}
	close(q.ch)
}

// AddConsumed 由消费方(worker)调用,累计已消费记录数
func (q *Queue) AddConsumed(n uint64) {
	q.consumed.Add(n)
}

// Stats 返回统计快照
func (q *Queue) Stats() QueueStats {
	return QueueStats{
		Enqueued: q.enqueued.Load(),
		Dropped:  q.dropped.Load(),
		Consumed: q.consumed.Load(),
	}
}
