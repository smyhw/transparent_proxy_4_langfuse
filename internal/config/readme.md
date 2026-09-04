# internal/config

配置包:负责 YAML 配置文件的解析、默认值填充与值域校验。

关键设计:
- 所有配置项均有默认值,`Load` 采用"默认值 → YAML 覆盖 → 校验"三步流程;
- `Duration` 与 `ByteSize` 是自定义类型,支持 `10s`、`4MiB` 等人类可读格式;
- 解析器合法名单以 `parser` 包为唯一来源(`parser.IsKnown`),避免两处维护漂移。
- `user_key_as_user_id`(默认关):开启后认证头(x-api-key/Authorization)纳入快照白名单,并作为 `langfuse.user.id` 上报。
