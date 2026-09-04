// queue_test.go 测试有界队列的 drop/block 语义、排空与统计。
package queue

import (
	"testing"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// TestTryEnqueueDrop 验证队列满时立即丢弃并计数
func TestTryEnqueueDrop(t *testing.T) {
	q := NewQueue(1)
	if !q.TryEnqueue(&record.Record{}) {
		t.Fatalf("首个入队应成功")
	}
	if q.TryEnqueue(&record.Record{}) {
		t.Fatalf("队列已满,第二次应失败")
	}
	st := q.Stats()
	if st.Enqueued != 1 || st.Dropped != 1 {
		t.Errorf("统计不符: %+v", st)
	}
}

// TestEnqueueBlockUnblocks 验证 block 策略:满时阻塞,消费后解阻
func TestEnqueueBlockUnblocks(t *testing.T) {
	q := NewQueue(1)
	q.Enqueue(&record.Record{}) // 填满队列
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Enqueue(&record.Record{}) // 应阻塞直到有消费者取走一条
	}()
	// 短暂等待:此时不应解阻
	select {
	case <-done:
		t.Fatalf("队列满时 Enqueue 不应立即返回")
	case <-time.After(50 * time.Millisecond):
	}
	// 消费一条后应解阻
	<-q.Ch()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("消费后 Enqueue 应解阻")
	}
}

// TestCloseDrains 验证 Close 后消费端仍能排空剩余记录
func TestCloseDrains(t *testing.T) {
	q := NewQueue(4)
	for i := 0; i < 3; i++ {
		if !q.TryEnqueue(&record.Record{URLPath: "p"}) {
			t.Fatalf("入队失败")
		}
	}
	q.Close()
	// for-range 应恰好取到 3 条后结束
	count := 0
	for range q.Ch() {
		count++
		q.AddConsumed(1)
	}
	if count != 3 {
		t.Errorf("排空数量 = %d,期望 3", count)
	}
	if q.Stats().Consumed != 3 {
		t.Errorf("consumed = %d,期望 3", q.Stats().Consumed)
	}
}

// TestTryEnqueueAfterClose 验证关闭后再投递被安全丢弃(不 panic)
func TestTryEnqueueAfterClose(t *testing.T) {
	q := NewQueue(1)
	q.Close()
	if q.TryEnqueue(&record.Record{}) {
		t.Fatalf("关闭后投递应失败")
	}
	q.Enqueue(&record.Record{}) // block 语义下关闭后也应安全返回
	if q.Stats().Dropped != 2 {
		t.Errorf("关闭后两次投递都应计为丢弃,实际 %d", q.Stats().Dropped)
	}
}
