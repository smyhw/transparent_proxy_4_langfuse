# internal/proxy

转发层:透明 HTTP 反向代理 + 旁路捕获 + 异步入队。

性能关键点(转发路径上的全部额外成本):
- 非候选请求零包装零缓冲纯直通,与原生 ReverseProxy 开销一致;
- 候选请求仅增加:读路径上的两次内存 append(Tee 旁路)、一次非阻塞 channel 投递、若干原子计数;
- 无锁、无整体缓冲、无每请求日志、无同步网络 IO;
- `FlushInterval: -1` 保证 SSE 逐块即时转发;`ResponseWriter` 透传 Flush/Hijack/Unwrap;
- 候选判定仅读 header(方法/路径后缀/Content-Type/Upgrade),零分配。
