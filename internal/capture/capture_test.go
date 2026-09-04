// capture_test.go 测试捕获层三件套:上限缓冲、旁路读取器、响应写入器包装。
package capture

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBufferLimit 验证超限截断语义:只存前 limit 字节,Write 恒返回完整长度
func TestBufferLimit(t *testing.T) {
	b := NewBuffer(10)
	n, err := b.Write([]byte("hello "))
	if err != nil || n != 6 {
		t.Fatalf("首次写入返回 (%d, %v),期望 (6, nil)", n, err)
	}
	n, err = b.Write([]byte("world"))
	if err != nil || n != 5 {
		t.Fatalf("超限写入仍应返回完整长度 5,实际 (%d, %v)", n, err)
	}
	if got := b.Bytes(); string(got) != "hello worl" {
		t.Errorf("存储内容 = %q,期望 %q", got, "hello worl")
	}
	if !b.Truncated() {
		t.Errorf("应标记为截断")
	}
	if b.Written() != 11 {
		t.Errorf("总字节数 = %d,期望 11", b.Written())
	}
	// 截断后再写入:只计数不存储
	b.Write([]byte("!!!"))
	if len(b.Bytes()) != 10 || b.Written() != 14 {
		t.Errorf("截断后写入行为错误: 存 %d 计 %d", len(b.Bytes()), b.Written())
	}
}

// TestBufferUnderLimit 验证未超限时的正常存储
func TestBufferUnderLimit(t *testing.T) {
	b := NewBuffer(100)
	b.Write([]byte("短内容"))
	if b.Truncated() {
		t.Errorf("未超限不应标记截断")
	}
	if string(b.Bytes()) != "短内容" {
		t.Errorf("内容不符: %q", b.Bytes())
	}
}

// TestTeeReadCloser 验证旁路捕获:数据原样透传、捕获一致、onFirst 仅一次
func TestTeeReadCloser(t *testing.T) {
	src := io.NopCloser(strings.NewReader("hello world"))
	sink := NewBuffer(100)
	calls := 0
	tee := NewTeeReadCloser(src, sink, func() { calls++ })

	// 小分块读取,验证分块边界不影响捕获
	var got bytes.Buffer
	buf := make([]byte, 3)
	for {
		n, err := tee.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != "hello world" {
		t.Errorf("透传内容 = %q", got.String())
	}
	if string(sink.Bytes()) != "hello world" {
		t.Errorf("捕获内容 = %q", sink.Bytes())
	}
	if calls != 1 {
		t.Errorf("onFirst 应只回调 1 次,实际 %d", calls)
	}
	if err := tee.Close(); err != nil {
		t.Errorf("Close 失败: %v", err)
	}
}

// flushProbe 记录 Flush 调用次数的响应写入器探针
type flushProbe struct {
	*httptest.ResponseRecorder
	flushes int
}

// Flush 记录调用并委托底层
func (f *flushProbe) Flush() {
	f.flushes++
	f.ResponseRecorder.Flush()
}

// TestResponseWriter 验证状态码记录、Flush/Unwrap 透传与 Hijack 报错
func TestResponseWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	probe := &flushProbe{ResponseRecorder: rec}
	w := NewResponseWriter(probe)

	// 显式 WriteHeader:记录首个状态码
	w.WriteHeader(http.StatusNotFound)
	if w.Status() != 404 {
		t.Errorf("Status = %d,期望 404", w.Status())
	}
	// 第二次 WriteHeader 不应覆盖首个状态码
	w.WriteHeader(http.StatusInternalServerError)
	if w.Status() != 404 {
		t.Errorf("首个状态码不应被覆盖: %d", w.Status())
	}
	// Flush 透传
	w.Flush()
	if probe.flushes != 1 {
		t.Errorf("Flush 未透传,调用次数 %d", probe.flushes)
	}
	// Unwrap 返回底层
	if w.Unwrap() != probe {
		t.Errorf("Unwrap 未返回底层写入器")
	}
	// httptest.Recorder 不支持 Hijack,应返回错误
	if _, _, err := w.Hijack(); err == nil {
		t.Errorf("非 Hijacker 应返回错误")
	}
	// 头透传
	w.Header().Set("X-Test", "1")
	if rec.Header().Get("X-Test") != "1" {
		t.Errorf("Header 未透传")
	}
}

// TestResponseWriterImplicit200 验证未显式 WriteHeader 时按 200 处理
func TestResponseWriterImplicit200(t *testing.T) {
	w := NewResponseWriter(httptest.NewRecorder())
	w.Write([]byte("body"))
	if w.Status() != 200 {
		t.Errorf("隐式状态码 = %d,期望 200", w.Status())
	}
}
