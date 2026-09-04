# internal/queue

队列包:转发完成后的流量快照与上报 worker 之间的有界缓冲。

关键设计:有界 channel 天然线程安全;`TryEnqueue`(drop)用非阻塞投递保证
转发协程零等待;`Enqueue`(block)是用户显式选择的折中;`Close` 只关投递端,
worker 以 for-range 排空剩余记录。
