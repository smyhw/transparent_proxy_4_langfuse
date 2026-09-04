// Package record 定义"流量快照"结构:一次 HTTP 转发完成后,异步解析与上报所需的全部数据。
// 生命周期:快照由代理转发协程创建并写入,入队后仅由 worker 协程读取,不存在并发访问。
package record

import (
	"net/http"
	"time"
)

// Record 是一次 HTTP 转发的完整快照
type Record struct {
	Method            string      // 请求方法(如 POST)
	URLPath           string      // 请求路径(如 /v1/chat/completions)
	RawQuery          string      // 查询串(暂未用于解析,保留备查)
	StatusCode        int         // 上游返回的状态码
	StartTime         time.Time   // 请求开始时刻(代理本地时钟)
	EndTime           time.Time   // 响应结束时刻(代理本地时钟)
	FirstResponseByte time.Time   // 首个响应字节到达时刻;零值表示无响应体
	RequestHeaders    http.Header // 仅白名单子集的请求头(默认不含敏感头,配置可扩展)
	ResponseHeaders   http.Header // 仅白名单子集的响应头
	RequestBody       []byte      // 捕获的请求体原始字节(可能为 gzip 压缩数据)
	ResponseBody      []byte      // 捕获的响应体原始字节(可能为 gzip 压缩数据)
	RequestTruncated  bool        // 请求体是否因超上限被截断
	ResponseTruncated bool        // 响应体是否因超上限被截断
}

// baseHeaderKeys 是恒定的头白名单:解析必需且不含敏感信息。
// 注意:Authorization、x-api-key 等敏感头默认不进入快照;仅当配置显式开启
// user_key_as_user_id(或把认证头名配为 user/session 头)时才经 extra 参数纳入。
var baseHeaderKeys = []string{"Content-Type", "Content-Encoding"}

// SnapshotHeaders 从 http.Header 中按白名单提取子集。
// extra 用于追加配置驱动的头名(如 user/session 头或认证头)。
func SnapshotHeaders(h http.Header, extra ...string) http.Header {
	if h == nil {
		return nil
	}
	keys := append(append([]string{}, baseHeaderKeys...), extra...)
	out := make(http.Header, len(keys))
	for _, k := range keys {
		if k == "" {
			continue
		}
		if vs := h.Values(k); len(vs) > 0 {
			out[k] = append([]string{}, vs...)
		}
	}
	return out
}
