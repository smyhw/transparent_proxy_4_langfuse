// sse_test.go 测试增量式 SSE 解码器的各类边界:多行 data、CRLF、BOM、分块喂入、事件名等。
package parser

import (
	"reflect"
	"testing"
)

// TestSSESingleEvent 验证单行 data 的基本解码
func TestSSESingleEvent(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("data: hello\n\n")), dec.finish()...)
	want := []sseEvent{{name: "message", data: "hello"}}
	if !reflect.DeepEqual(events, want) {
		t.Errorf("事件 = %+v,期望 %+v", events, want)
	}
}

// TestSSEMultiLineData 验证多行 data 以换行拼接
func TestSSEMultiLineData(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("data: a\ndata: b\ndata: c\n\n")), dec.finish()...)
	if len(events) != 1 || events[0].data != "a\nb\nc" {
		t.Errorf("多行 data 拼接错误: %+v", events)
	}
}

// TestSSECRLF 验证 CRLF 行尾兼容
func TestSSECRLF(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("data: x\r\n\r\n")), dec.finish()...)
	if len(events) != 1 || events[0].data != "x" {
		t.Errorf("CRLF 处理错误: %+v", events)
	}
}

// TestSSEBOM 验证流首 BOM 剥离
func TestSSEBOM(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("\ufeffdata: y\n\n")), dec.finish()...)
	if len(events) != 1 || events[0].data != "y" {
		t.Errorf("BOM 处理错误: %+v", events)
	}
}

// TestSSEChunkedFeed 验证任意分块喂入(残尾跨分块)不影响解码结果
func TestSSEChunkedFeed(t *testing.T) {
	dec := &sseDecoder{}
	var events []sseEvent
	// 把完整流切成不规则分块喂入(覆盖残尾跨分块场景)
	for _, chunk := range [][]byte{[]byte("da"), []byte("ta: 第一"), []byte("行\ndata"), []byte(": 第二行\n\n: 注释"), []byte("行\n\nda"), []byte("ta: 第三行\n\n")} {
		events = append(events, dec.feed(chunk)...)
	}
	events = append(events, dec.finish()...)
	if len(events) != 2 {
		t.Fatalf("事件数 = %d,期望 2: %+v", len(events), events)
	}
	if events[0].data != "第一行\n第二行" {
		t.Errorf("事件0 data = %q", events[0].data)
	}
	if events[1].data != "第三行" {
		t.Errorf("事件1 data = %q", events[1].data)
	}
}

// TestSSEEventName 验证 event: 字段解析与缺省名
func TestSSEEventName(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("event: delta\ndata: x\n\n")), dec.finish()...)
	if len(events) != 1 || events[0].name != "delta" {
		t.Errorf("事件名 = %+v,期望 delta", events)
	}
	// 缺省名为 message
	dec = &sseDecoder{}
	events = append(dec.feed([]byte("data: y\n\n")), dec.finish()...)
	if events[0].name != "message" {
		t.Errorf("缺省事件名 = %q,期望 message", events[0].name)
	}
}

// TestSSEFinishFlushPending 验证流结束(无空行结尾)时 finish 冲刷最后一个事件
func TestSSEFinishFlushPending(t *testing.T) {
	dec := &sseDecoder{}
	events := dec.feed([]byte("data: tail"))
	if len(events) != 0 {
		t.Fatalf("未结束前不应产出事件")
	}
	events = dec.finish()
	if len(events) != 1 || events[0].data != "tail" {
		t.Errorf("finish 冲刷错误: %+v", events)
	}
}

// TestSSEIpgyIdRetryIgnored 验证 id:/retry:/注释行被忽略
func TestSSEIdRetryIgnored(t *testing.T) {
	dec := &sseDecoder{}
	events := append(dec.feed([]byte("id: 42\nretry: 100\n: 心跳\ndata: z\n\n")), dec.finish()...)
	if len(events) != 1 || events[0].data != "z" {
		t.Errorf("id/retry/注释处理错误: %+v", events)
	}
}
