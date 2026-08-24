> [!NOTE]
本项目大量使用了vibe coding，包含AI生成的代码

# 简介

本项目实现了适用于langfuse的透明代理，即允许将langfuse直接插入api提供者和客户端调用者之间，无需配置或更改原有key和调用逻辑。

---

功能很简单，本项目将监听一个端口，并将收到的所有流量转发给上游模型接口，不做任何修改。每个连接结束后，将嗅探其中的模型调用，若存在支持的协议数据(Completions,Responses或a/的协议)，则发给langfuse。

---
langfuse官方推荐使用litellm，但实现透传问题很多，例如目前在openai接口格式下无法透传用户密钥，必须折腾虚拟密钥:(

---

# 特性
* 基于golang，所以单文件开箱即用，且适用于所有平台！
  - *还是得有一个简单的配置文件负责定义上游地址和langfuse信息*
* 透明 LLM 代理
  - 所有发往监听地址的 HTTP 流量**原封不动**转发到上游目标地址
* 转发性能为最高优先级
  - 请求结束后**异步**解析流量
  - 若能识别为任何支持的 LLM API 调用
  - 则通过[Langfuse 原生 OpenTelemetry 机制](https://langfuse.com/integrations/native/opentelemetry)上报观测数据
  - 识别不了或出错的流量也照常转发，不会影响正常通讯

# 构建与运行

```bash
CGO_ENABLED=0 go build -o proxy .          # 构建二进制
cp config.example.yml config.yml   # 准备配置(修改上游地址与 Langfuse 密钥)
./proxy -config config.yml   # 启动
```


# 配置

完整示例见 [config.example.yml](config.example.yml),所有字段均可省