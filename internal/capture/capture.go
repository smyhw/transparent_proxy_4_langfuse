// Package capture 提供转发路径上的"旁路捕获"组件。
// 核心原则:转发字节流原样透传、零提前读取、零整体缓冲;捕获只是读路径上的一次内存 append,
// 超限后只计数不存储,上游对捕获的存在完全无感知。
package capture

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
)

// Buffer 是带上限的捕获缓冲:达到上限后丢弃新数据但保持字节计数
type Buffer struct {
	buf     bytes.Buffer // 实际存储的字节(≤ limit)
	limit   int          // 存储上限(字节)
	full    bool         // 是否已触发截断
	written int64        // 流经的总字节数(含被丢弃部分,用于统计)
}

// NewBuffer 创建带上限的捕获缓冲
func NewBuffer(limit int) *Buffer {
	return &Buffer{limit: limit}
}

// Write 追加数据:未超限时存储,超限后只计数;恒返回 len(p) 保证上游写入语义不变
func (b *Buffer) Write(p []byte) (int, error) {
	b.written += int64(len(p))
	if b.full {
		return len(p), nil // 已截断:只计数,不再存储
	}
	remain := b.limit - b.buf.Len()
	if remain >= len(p) {
		b.buf.Write(p)
		return len(p), nil
	}
	b.buf.Write(p[:remain])
	b.full = true // 达到上限:此后只计数不再存储
	return len(p), nil
}

// Truncated 报告捕获内容是否因超上限被截断
func (b *Buffer) Truncated() bool { return b.full }

// Written 返回流经的总字节数(含被丢弃部分)
func (b *Buffer) Written() int64 { return b.written }

// Bytes 返回已捕获的字节;切片所有权移交调用方(零拷贝,此后不得再写入)
func (b *Buffer) Bytes() []byte { return b.buf.Bytes() }

// teeReadCloser 在读取路径上旁路捕获:读到的每个分块原样返回,同时追加到 sink
type teeReadCloser struct {
	src       io.ReadCloser // 底层数据源
	sink      *Buffer       // 旁路捕获缓冲
	onFirst   func()        // 首次读到数据时回调一次(用于记录首字节时刻)
	firstOnce sync.Once     // 保证 onFirst 只触发一次
}

// NewTeeReadCloser 创建旁路捕获读取器;onFirst 可为 nil
func NewTeeReadCloser(src io.ReadCloser, sink *Buffer, onFirst func()) *teeReadCloser {
	return &teeReadCloser{src: src, sink: sink, onFirst: onFirst}
}

// Read 先读底层再旁路存储,返回值与原读完全一致,不改变任何字节语义
func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 {
		if t.onFirst != nil {
			t.firstOnce.Do(t.onFirst)
		}
		t.sink.Write(p[:n]) // 捕获:仅追加到旁路缓冲,不影响返回给调用方的数据
	}
	return n, err
}

// Close 关闭底层数据源
func (t *teeReadCloser) Close() error { return t.src.Close() }

// ResponseWriter 包装底层 ResponseWriter 以记录响应状态码,
// 同时透传 Flush/Hijack/Unwrap 能力,保证流式(SSE)与协议升级场景不受影响
type ResponseWriter struct {
	rw     http.ResponseWriter // 底层写入器
	status int                 // 首个写入的状态码
	wrote  bool                // 是否已调用 WriteHeader
}

// NewResponseWriter 创建记录状态码的响应写入器包装
func NewResponseWriter(rw http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{rw: rw}
}

// Header 透传响应头
func (w *ResponseWriter) Header() http.Header { return w.rw.Header() }

// WriteHeader 记录首个状态码并透传
func (w *ResponseWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.rw.WriteHeader(code)
}

// Write 写入响应体;首次写入隐式 WriteHeader(200)
func (w *ResponseWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.status = http.StatusOK
		w.wrote = true
	}
	return w.rw.Write(p)
}

// Flush 委托底层刷新缓冲;SSE 逐块即时推送的关键
func (w *ResponseWriter) Flush() {
	if f, ok := w.rw.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack 委托底层连接接管(WebSocket 等协议升级场景)
func (w *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.rw.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("底层 ResponseWriter 不支持 Hijack")
	}
	return h.Hijack()
}

// Unwrap 返回底层写入器,供 http.ResponseController 使用
func (w *ResponseWriter) Unwrap() http.ResponseWriter { return w.rw }

// Status 返回最终状态码(从未 WriteHeader 时按约定视为 200)
func (w *ResponseWriter) Status() int {
	if !w.wrote {
		return http.StatusOK
	}
	return w.status
}
