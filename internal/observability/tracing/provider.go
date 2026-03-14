// Package tracing configures an OpenTelemetry TracerProvider that exports
// spans to an OTLP-compatible backend (Jaeger, Grafana Tempo, etc.).
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mikey-austin/tierfs/internal/config"
)

// Provider wraps the SDK TracerProvider and exposes a Shutdown hook.
type Provider struct {
	sdk *sdktrace.TracerProvider
}

// New builds a Provider from TracingConfig and registers it as the global
// OTel tracer. Returns a no-op provider if tracing is disabled.
func New(cfg config.TracingConfig) (*Provider, error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return &Provider{}, nil
	}

	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("tracing.endpoint is required when tracing is enabled")
	}

	dialOpts := []grpc.DialOption{}
	if cfg.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	exp, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithDialOption(dialOpts...),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String("dev"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	sampler := sdktrace.AlwaysSample()
	if cfg.SampleRate < 1.0 {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	return &Provider{sdk: tp}, nil
}

// Shutdown flushes pending spans and releases resources. Call from main()
// deferred after all work is done.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.sdk == nil {
		return nil
	}
	return p.sdk.Shutdown(ctx)
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
