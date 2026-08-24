[中文文档](readme-cn.md)

> [!NOTE]
> This project makes heavy use of vibe coding and contains AI-generated code

# Introduction

This project implements a transparent proxy for langfuse, which allows langfuse to be inserted directly between API providers and client callers, without any configuration or changes to existing keys and calling logic.

---

The functionality is simple: this project listens on a port and forwards all received traffic to the upstream model API without any modification. After each connection ends, it sniffs the model calls within; if supported protocol data (Completions, Responses, or a/ protocols) is present, it is sent to langfuse.

---

langfuse officially recommends using litellm, but implementing transparent passthrough has many issues; for example, under the OpenAI interface format user keys currently cannot be passed through transparently, requiring fiddling with virtual keys :(

---

# Features
* Built with golang, so it's a single-file, out-of-the-box solution that works on all platforms!
  - *You still need a simple config file to define the upstream address and langfuse information*
* Transparent LLM proxy
  - All HTTP traffic sent to the listening address is forwarded to the upstream target address **unchanged**
* Forwarding performance is the top priority
  - Traffic is parsed **asynchronously** after the request completes
  - If it can be identified as any supported LLM API call
  - Then observability data is reported via the [Langfuse native OpenTelemetry mechanism](https://langfuse.com/integrations/native/opentelemetry)
  - Traffic that cannot be identified or errors also get forwarded as usual, without affecting normal communication

# Build & Run

```bash
CGO_ENABLED=0 go build -o proxy .          # Build the binary
cp config.example.yml config.yml   # Prepare config (edit upstream address & Langfuse keys)
./proxy -config config.yml   # Start
```


# Configuration

See [config.example.yml](config.example.yml) for a complete example; all fields are optional
