// Package proxy 实现透明转发层:所有 HTTP 流量原封不动转发到上游目标,
// 对"候选请求"(可能为 LLM API 调用的流量)启用旁路捕获,请求结束后异步投递上报队列。
// 性能关键点:非候选请求零包装零缓冲纯直通;候选请求仅增加读路径上的旁路 append
// 与一次非阻塞入队,转发字节流本身零提前读取、零整体缓冲、零延迟。
package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/capture"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/parser"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/queue"
	"github.com/smyhw/transparent_proxy_4_langfuse/internal/record"
)

// recKey 是请求上下文中存放捕获状态的键(私有类型避免冲突)
type recKey struct{}

// captureState 是一个候选请求的捕获上下文,经请求上下文传给 ModifyResponse 回调
type captureState struct {
	rec     *record.Record  // 正在填充的流量快照
	respBuf *capture.Buffer // 响应体旁路缓冲
}

// proxyStats 是转发层的原子统计
type proxyStats struct {
	forwarded  atomic.Uint64 // 转发的全部请求数
	candidates atomic.Uint64 // 启用捕获的候选请求数
}

// Proxy 是透明转发处理器
type Proxy struct {
	rp           *httputil.ReverseProxy // 标准库反向代理(处理 hop-by-hop 头、升级、流式刷新)
	cfg          *config.Config         // 全局配置
	queue        *queue.Queue           // 上报队列
	enabled      bool                   // 是否已启用 langfuse(决定是否捕获)
	suffixes     []string               // 候选路径后缀(解析器提供 + 配置追加)
	extraHeaders []string               // 附加头白名单(配置的 user/session 头名;开启 user_key_as_user_id 时含认证头)
	targetScheme string                 // 上游协议(http/https)
	targetHost   string                 // 上游主机
	stats        proxyStats             // 原子统计
}

// New 创建转发处理器;reg 提供候选路径后缀,cfg 提供其余装配参数
func New(cfg *config.Config, q *queue.Queue, reg *parser.Registry) (*Proxy, error) {
	target, err := url.Parse(cfg.Target.URL)
	if err != nil {
		return nil, fmt.Errorf("目标地址无效: %w", err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("目标地址协议必须是 http 或 https: %q", target.Scheme)
	}
	if target.Host == "" {
		return nil, errors.New("目标地址缺少主机名")
	}
	p := &Proxy{
		cfg:          cfg,
		queue:        q,
		enabled:      cfg.LangfuseEnabled(),
		targetScheme: target.Scheme,
		targetHost:   target.Host,
	}
	// 候选路径 = 已启用解析器覆盖的后缀 + 配置的额外路径
	p.suffixes = append(p.suffixes, reg.PathSuffixes()...)
	p.suffixes = append(p.suffixes, cfg.Parser.PathInclude...)
	// 附加头白名单:配置的 user/session 头名,以及可选开启的认证头
	if cfg.Langfuse.UserHeader != "" {
		p.extraHeaders = append(p.extraHeaders, cfg.Langfuse.UserHeader)
	}
	if cfg.Langfuse.SessionHeader != "" {
		p.extraHeaders = append(p.extraHeaders, cfg.Langfuse.SessionHeader)
	}

	// 开启 user_key_as_user_id 时,认证头纳入快照白名单(默认不含敏感头)
	if cfg.Langfuse.UserKeyAsUserID {
		p.extraHeaders = append(p.extraHeaders, "X-Api-Key", "Authorization")
	}

	// 上游连接池:保活连接避免流式请求反复建连
	transport := &http.Transport{
		MaxIdleConnsPerHost: cfg.Target.MaxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		// 其余字段使用零值安全默认(http.DefaultTransport 的关键参数)
	}
	rp := &httputil.ReverseProxy{
		// 使用现代 Rewrite API(Go 1.20+):仅改写目标,并按配置追加 X-Forwarded-For
		Rewrite: func(pr *httputil.ProxyRequest) {
			p.director(pr.Out)
			if cfg.Target.SetXForwardedFor {
				pr.SetXForwarded() // 标准代理行为:为转发请求设置 X-Forwarded-For
			}
		},
		Transport:      transport,
		ModifyResponse: p.modifyResponse,
		ErrorHandler:   p.errorHandler,
		FlushInterval:  -1, // 立即刷新:SSE 流式响应逐块即时转发到客户端
	}
	p.rp = rp
	return p, nil
}

// ServeHTTP 处理一个请求:候选请求旁路捕获,其余纯直通
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.stats.forwarded.Add(1)
	if !p.isCandidate(r) {
		p.rp.ServeHTTP(w, r) // 非候选:零包装零缓冲,与原生反向代理开销一致
		return
	}
	p.stats.candidates.Add(1)

	// 构造流量快照,记录请求侧信息
	rec := &record.Record{
		Method:         r.Method,
		URLPath:        r.URL.Path,
		RawQuery:       r.URL.RawQuery,
		StartTime:      time.Now(),
		RequestHeaders: record.SnapshotHeaders(r.Header, p.extraHeaders...),
	}
	reqBuf := capture.NewBuffer(int(p.cfg.Capture.MaxRequestBytes))
	respBuf := capture.NewBuffer(int(p.cfg.Capture.MaxResponseBytes))
	// 请求体旁路捕获:转发的同时边读边存,不提前读取、不改变字节语义
	if r.Body != nil {
		r.Body = capture.NewTeeReadCloser(r.Body, reqBuf, nil)
	}
	// 通过请求上下文把本请求的捕获状态传给 ModifyResponse 回调
	ctx := context.WithValue(r.Context(), recKey{}, &captureState{rec: rec, respBuf: respBuf})
	r = r.WithContext(ctx)

	// 包装响应写入器以记录状态码(Flush/Hijack/Unwrap 全部透传)
	rw := capture.NewResponseWriter(w)
	p.rp.ServeHTTP(rw, r)

	// 转发结束:快照数据移交给队列,之后仅 worker 读取
	rec.EndTime = time.Now()
	rec.StatusCode = rw.Status()
	rec.RequestBody, rec.RequestTruncated = reqBuf.Bytes(), reqBuf.Truncated()
	rec.ResponseBody, rec.ResponseTruncated = respBuf.Bytes(), respBuf.Truncated()
	p.enqueue(rec)
}

// isCandidate 判定请求是否可能为可识别的 LLM API 调用(仅读 header,零分配)
func (p *Proxy) isCandidate(r *http.Request) bool {
	if !p.enabled {
		return false // 未配置 Langfuse 密钥:完全纯转发
	}
	if r.Method != http.MethodPost {
		return false
	}
	if isUpgrade(r.Header) {
		return false // 升级请求(如 WebSocket 握手)直通,不捕获
	}
	path := r.URL.Path
	if suffixMatch(path, p.cfg.Parser.PathExclude) {
		return false // 命中排除路径:仅转发
	}
	if !suffixMatch(path, p.suffixes) {
		return false // 路径未命中任何解析器:仅转发
	}
	if !parser.AcceptsJSON(r.Header) {
		return false // Content-Type 非 JSON(且非缺失):不做无谓捕获
	}
	return true
}

// enqueue 将快照投递上报队列;策略按配置选择,默认 drop 保证转发协程零等待
func (p *Proxy) enqueue(rec *record.Record) {
	if p.cfg.Queue.FullPolicy == "block" {
		p.queue.Enqueue(rec) // block:队列满时阻塞(用户显式选择的折中,会拖慢转发)
		return
	}
	p.queue.TryEnqueue(rec) // drop:满则立即丢弃,计数在 queue 内部完成
}

// director 重写转发目标:仅修改 Scheme/Host(以及按配置处理 Host 头),不动任何业务头
func (p *Proxy) director(req *http.Request) {
	req.URL.Scheme = p.targetScheme
	req.URL.Host = p.targetHost
	if !p.cfg.Target.PreserveHost {
		req.Host = p.targetHost // 默认:Host 头改为上游主机
	}
	// 注意:hop-by-hop 头由标准库按 RFC 剥除,属协议正确性要求而非业务改动
}

// modifyResponse 在收到上游响应时包装响应体做旁路捕获;101 升级响应原样放行
func (p *Proxy) modifyResponse(resp *http.Response) error {
	st, ok := resp.Request.Context().Value(recKey{}).(*captureState)
	if !ok {
		return nil // 非候选请求(或上下文缺失):不捕获
	}
	st.rec.ResponseHeaders = record.SnapshotHeaders(resp.Header, p.extraHeaders...)
	if resp.StatusCode == http.StatusSwitchingProtocols {
		return nil // 协议升级:不包装响应体,保证双端直拷
	}
	resp.Body = capture.NewTeeReadCloser(resp.Body, st.respBuf, func() {
		if st.rec.FirstResponseByte.IsZero() {
			st.rec.FirstResponseByte = time.Now() // 记录首字节时刻(流式性能指标)
		}
	})
	return nil
}

// errorHandler 处理上游连接失败等转发错误:记录日志后返回 502
func (p *Proxy) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	slog.Warn("转发到上游失败", "path", r.URL.Path, "error", err)
	w.WriteHeader(http.StatusBadGateway)
}

// ProxyStats 是转发统计快照(供周期日志输出)
type ProxyStats struct {
	Forwarded  uint64 // 转发的全部请求数
	Candidates uint64 // 启用捕获的候选请求数
}

// Stats 返回转发统计快照
func (p *Proxy) Stats() ProxyStats {
	return ProxyStats{
		Forwarded:  p.stats.forwarded.Load(),
		Candidates: p.stats.candidates.Load(),
	}
}

// suffixMatch 判断路径是否与任一后缀匹配(后缀为 "/x" 时也接受完全相等)
func suffixMatch(path string, suffixes []string) bool {
	for _, s := range suffixes {
		if s != "" && strings.HasSuffix(path, s) {
			return true
		}
	}
	return false
}

// isUpgrade 判断请求是否为协议升级请求(如 WebSocket 握手)
func isUpgrade(h http.Header) bool {
	return headerContainsToken(h, "Connection", "upgrade")
}

// headerContainsToken 判断头值中是否包含指定 token(大小写不敏感,支持逗号分隔列表)
func headerContainsToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}
