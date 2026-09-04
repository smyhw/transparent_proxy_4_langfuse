# internal/record

流量快照包:定义转发完成后异步上报所需的数据结构 `Record`,以及头白名单快照逻辑。

关键设计:头快照只保留 `Content-Type`/`Content-Encoding` 与配置的 user/session 头,
`Authorization` 等敏感头默认不进入快照;仅当配置开启 `user_key_as_user_id`
(或把认证头名配为 user/session 头)时才纳入。
