# internal/capture

捕获包:转发路径上的旁路捕获三件套——`Buffer`(带上限的捕获缓冲)、
`NewTeeReadCloser`(读路径旁路捕获)、`ResponseWriter`(记录状态码并透传 Flush/Hijack/Unwrap)。

性能要点:字节流原样透传、零提前读取、零整体缓冲;超限后只计数不存储,
`Write` 恒返回完整长度,上游对捕获的存在完全无感知。
