// sse.go 实现增量式 SSE(Server-Sent Events)解码器:
// 把任意分块喂入的字节流切分为完整事件,供各解析器重建流式 LLM 响应。
package parser

import (
	"bytes"
	"strings"
)

// sseEvent 是一条完整的 SSE 事件
type sseEvent struct {
	name string // event: 字段的值,缺省为 "message"
	data string // 多行 data: 以换行拼接后的载荷
}

// sseDecoder 是增量式 SSE 解码器:内部缓冲残尾,支持任意大小的分块喂入
type sseDecoder struct {
	buf         bytes.Buffer // 尚未构成完整事件的残余字节
	evName      string       // 当前事件累计的 event 字段
	evData      []string     // 当前事件累计的 data 行
	bomStripped bool         // 是否已剥离流首的 BOM(只处理一次)
}

// feed 喂入一个分块,返回其中完整的事件;不完整的数据留在内部缓冲
func (d *sseDecoder) feed(chunk []byte) []sseEvent {
	d.buf.Write(chunk)
	var events []sseEvent
	for {
		line, ok := d.readLine()
		if !ok {
			break // 没有完整行:等待下一个分块
		}
		events = append(events, d.handleLine(line)...)
	}
	return events
}

// finish 在流结束时调用:把缓冲中未以换行结尾的残尾作为最后一行处理,
// 并冲刷未完成的最后一个事件
func (d *sseDecoder) finish() []sseEvent {
	var events []sseEvent
	if d.buf.Len() > 0 {
		line := d.buf.String() // 流结束即视为行结束
		d.buf.Reset()
		events = append(events, d.handleLine(line)...)
	}
	if len(d.evData) > 0 {
		events = append(events, d.flush())
	}
	return events
}

// handleLine 处理一行:剥离 BOM/CRLF,按前缀分发到事件字段累积;
// 空行触发事件结束(返回该事件)
func (d *sseDecoder) handleLine(line string) []sseEvent {
	// 流首 BOM 剥离(部分上游会在 SSE 流开头输出 UTF-8 BOM)
	if !d.bomStripped {
		line = strings.TrimPrefix(line, "\ufeff")
		d.bomStripped = true
	}
	line = strings.TrimSuffix(line, "\r") // 兼容 CRLF
	switch {
	case line == "":
		// 空行=事件结束;data 为空的事件(如纯注释)直接丢弃
		if len(d.evData) > 0 {
			return []sseEvent{d.flush()}
		}
	case strings.HasPrefix(line, "event:"):
		d.evName = strings.TrimSpace(line[len("event:"):])
	case strings.HasPrefix(line, "data:"):
		// 规范:值前恰好一个空格被移除;TrimSpace 更宽松,可接受
		d.evData = append(d.evData, strings.TrimSpace(line[len("data:"):]))
	}
	// 其余字段(id:/retry:/以 ":" 开头的注释行)按规范忽略
	return nil
}

// flush 把当前累计的 data 行合并为一个事件并重置状态
func (d *sseDecoder) flush() sseEvent {
	ev := sseEvent{name: d.evName, data: strings.Join(d.evData, "\n")}
	if ev.name == "" {
		ev.name = "message"
	}
	d.evName = ""
	d.evData = d.evData[:0]
	return ev
}

// readLine 从内部缓冲取出一行(不含换行符);没有完整行时返回 ok=false
func (d *sseDecoder) readLine() (line string, ok bool) {
	data := d.buf.Bytes()
	idx := bytes.IndexByte(data, '\n')
	if idx < 0 {
		return "", false
	}
	line = string(data[:idx])
	d.buf.Next(idx + 1) // 消费该行(含换行符)
	return line, true
}
