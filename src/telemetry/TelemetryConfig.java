package telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;

//import io.opentelemetry.sdk.resources.Resource;
//import io.opentelemetry.semconv.ResouceAttributes;

/**
 * TelemetryConfig - Configures OpenTelemetry for tracing.
 * Central place to configure and access OpenTelemetry for this app
 */

public final class TelemetryConfig {
    private static Tracer tracer;

    private TelemetryConfig() {
        // private constructor to prevent instantiation
        // no instances
    }

    /**
     * Initialize OpenTelemetry once per process.
     *
     * @param serviceName the logical name of the service/application/program (e.g. "file-client", :file-server")
     * @param sampleRatio 1.0 for AlwaysOn, or 0.0-1.0 for probabilistic sampling
     */

    public static synchronized void init(String serviceName, double sampleRatio) {
        if (tracer != null) {
            // already initialized
            return;
        }

        // clamp to a sane range
        if (sampleRatio >= 1.0) {
            sampleRatio = 1.0;
        } else if (sampleRatio <= 0.0) {
            sampleRatio = 0.1;
        }

        // Export traces to an OTLP endpoint (OpenTelemetry Collector on localhost:4317)
//        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
//                .setEndpoint("http://localhost:4317")
//                .build();



        Sampler sampler = (sampleRatio >= 1.0)
                ? Sampler.alwaysOn()
                : Sampler.traceIdRatioBased(sampleRatio);

        LoggingSpanExporter spanExporter = new LoggingSpanExporter();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setSampler(sampler)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        // tracer name + serviceName just so they're distinguishable in the UI
        tracer = openTelemetry.getTracer("file-transfer-" + serviceName);
    }

    /**
     * get the share Tracer instance
     */
    public static Tracer tracer() {
        if (tracer == null) {
            throw new IllegalStateException("TelemetryConfig.init() must be called before tracer()");
        }
        return tracer;
    }
}


