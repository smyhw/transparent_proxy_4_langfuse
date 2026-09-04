// Package telemetry 负责把解析结果构建为 OpenTelemetry span 并上报 Langfuse。
// 接入方式为 Langfuse 原生 OTel 机制:OTLP over HTTP(protobuf)直达
// {endpoint}/api/public/otel/v1/traces,Basic Auth(公钥:密钥)+
// x-langfuse-ingestion-version 请求头。上报全程异步,不触碰转发路径。
package telemetry

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/smyhw/transparent_proxy_4_langfuse/internal/config"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.36.0"
)

// tracesPath 是 Langfuse 的 traces 专用 OTLP 端点路径
const tracesPath = "/api/public/otel/v1/traces"

// NewTracerProvider 组装 OTLP HTTP exporter 与批量处理器,返回 TracerProvider。
// 调用方在停机时必须调用 Shutdown 以排空批量队列。
func NewTracerProvider(cfg *config.LangfuseConfig) (*sdktrace.TracerProvider, error) {
	// 端点:配置的根地址 + traces 专用路径
	endpoint := strings.TrimRight(cfg.Endpoint, "/") + tracesPath
	// 认证:Langfuse 要求 HTTP Basic Auth(用户名=公钥,密码=密钥)
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.PublicKey+":"+cfg.SecretKey))
	headers := map[string]string{
		"Authorization":                auth,
		"x-langfuse-ingestion-version": strconv.Itoa(cfg.IngestionVersion), // 缺失会导致新数据模型延迟 10 分钟
	}
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithTimeout(cfg.ExportTimeout.Duration()),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 OTLP exporter 失败: %w", err)
	}
	// 资源属性:归入 Langfuse trace metadata
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(cfg.ServiceName),
		attribute.String("deployment.environment", cfg.Environment),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(cfg.Batch.Timeout.Duration()), // 批量等待:延迟与吞吐的平衡
			sdktrace.WithMaxExportBatchSize(cfg.Batch.MaxSize),
			sdktrace.WithMaxQueueSize(cfg.Batch.QueueSize), // SDK 队列满时丢最旧 span(内存有界)
		),
		sdktrace.WithResource(res),
	)
	return tp, nil
}
