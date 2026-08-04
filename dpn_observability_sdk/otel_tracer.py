# Copyright 2026 DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# +---------+----------------------------------------------------------+---------------+-------------+
# | Version | Description                                              | Change Owner  | Change Date |
# +---------+----------------------------------------------------------+---------------+-------------+
# | 1.0.0   | Initial version                                          | DSI Team      | 2026-06-26  |
# +---------+----------------------------------------------------------+---------------+-------------+
"""
OpenTelemetry Tracer Utility

This module provides a centralized tracer configuration for distributed tracing
across the data pipeline services.

Features:
- OTLP gRPC exporter for sending traces to OpenTelemetry Collector
- Resource attributes for service identification
- Batch span processor for efficient export
- Trace context propagation support

Usage:
    from utils.otel_tracer import get_tracer
    
    tracer = get_tracer()
    with tracer.start_as_current_span("operation_name") as span:
        span.set_attribute("key", "value")
        # Your code here
"""

import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

# Global tracer provider instance
_tracer_provider: Optional[TracerProvider] = None


def setup_tracing(
    service_name: Optional[str] = None,
    service_version: Optional[str] = None,
    otlp_endpoint: Optional[str] = None,
    environment: Optional[str] = None,
) -> TracerProvider:
    """
    Initialize OpenTelemetry tracing with OTLP exporter.
    
    This function should be called once at application startup.
    
    Args:
        service_name: Name of the service (defaults to SERVICE_NAME env var)
        service_version: Version of the service (defaults to SERVICE_VERSION env var)
        otlp_endpoint: OTLP collector endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var)
        environment: Deployment environment (defaults to ENVIRONMENT env var)
    
    Returns:
        TracerProvider: Configured tracer provider
    
    Example:
        setup_tracing(
            service_name="producer-schema-mapper",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            environment="development"
        )
    """
    global _tracer_provider
    
    # Get configuration from environment or parameters
    service_name = service_name or os.getenv("SERVICE_NAME", "unknown-service")
    service_version = service_version or os.getenv("SERVICE_VERSION", "0.0.0")
    otlp_endpoint = otlp_endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "dpn-otel-collector.ns-dpn-health-01.svc.cluster.local:4317")
    environment = environment or os.getenv("ENVIRONMENT", "development")
    
    # Create resource with service information
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        "deployment.environment": environment,
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.language": "python",
    })
    
    # Create tracer provider
    _tracer_provider = TracerProvider(resource=resource)
    
    # Configure OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "true").lower() == "true",
    )
    
    # Add batch span processor for efficient export
    span_processor = BatchSpanProcessor(otlp_exporter)
    _tracer_provider.add_span_processor(span_processor)
    
    # Set as global tracer provider
    trace.set_tracer_provider(_tracer_provider)
    
    return _tracer_provider


def get_tracer(name: Optional[str] = None) -> trace.Tracer:
    """
    Get a tracer instance.
    
    If tracing hasn't been set up, this will initialize it with default settings.
    
    Args:
        name: Name for the tracer (defaults to calling module name)
    
    Returns:
        Tracer: OpenTelemetry tracer instance
    
    Example:
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span("my_operation"):
            # Your code here
            pass
    """
    global _tracer_provider
    
    # Initialize tracing if not already done
    if _tracer_provider is None:
        setup_tracing()
    
    # Get tracer name from parameter or calling module
    tracer_name = name or __name__
    
    return trace.get_tracer(tracer_name)


def shutdown_tracing():
    """
    Shutdown tracing and flush any pending spans.
    
    This should be called before application exit to ensure all spans are exported.
    
    Example:
        import atexit
        atexit.register(shutdown_tracing)
    """
    global _tracer_provider
    
    if _tracer_provider is not None:
        _tracer_provider.shutdown()
        _tracer_provider = None


# ============================================================================
# Class-based API (Wrapper for backward compatibility)
# ============================================================================

class OtelTracer:
    """
    OpenTelemetry Tracer Factory (Class-based API).
    
    This class provides a class-based interface that wraps the function-based
    API above. It maintains compatibility with code that expects a class-based
    interface as specified in the implementation guide.
    
    Usage:
        from utils.otel_tracer import OtelTracer
        
        # Initialize at startup
        tracer = OtelTracer.initialize(
            service_name="my-service",
            service_version="1.0.0"
        )
        
        # Get tracer later
        tracer = OtelTracer.get_tracer(__name__)
    """
    
    _initialized: bool = False
    
    @classmethod
    def initialize(
        cls,
        service_name: Optional[str] = None,
        service_version: Optional[str] = None,
        otlp_endpoint: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> trace.Tracer:
        """
        Initialize OpenTelemetry tracing.
        
        This is a convenience method that wraps setup_tracing() and returns
        a tracer instance.
        
        Args:
            service_name: Name of the service (defaults to SERVICE_NAME env var)
            service_version: Version of the service (defaults to SERVICE_VERSION env var)
            otlp_endpoint: OTLP collector endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var)
            environment: Deployment environment (defaults to ENVIRONMENT env var)
        
        Returns:
            trace.Tracer: Configured tracer instance
        
        Example:
            tracer = OtelTracer.initialize(
                service_name="producer-schema-mapper",
                service_version="1.0.0",
                otlp_endpoint="http://otel-collector:4317",
                environment="development"
            )
        """
        if not cls._initialized:
            setup_tracing(
                service_name=service_name,
                service_version=service_version,
                otlp_endpoint=otlp_endpoint,
                environment=environment,
            )
            cls._initialized = True
        
        return trace.get_tracer(__name__)
    
    @classmethod
    def get_tracer(cls, name: Optional[str] = None) -> trace.Tracer:
        """
        Get a tracer instance.
        
        If tracing hasn't been initialized, this will initialize it with
        default settings.
        
        Args:
            name: Name for the tracer (defaults to __name__)
        
        Returns:
            trace.Tracer: OpenTelemetry tracer instance
        
        Example:
            tracer = OtelTracer.get_tracer(__name__)
            with tracer.start_as_current_span("my_operation"):
                # Your code here
                pass
        """
        if not cls._initialized:
            cls.initialize()
        
        return get_tracer(name)
    
    @classmethod
    def shutdown(cls):
        """
        Shutdown tracing and flush any pending spans.
        
        This is a convenience method that wraps shutdown_tracing().
        
        Example:
            import atexit
            atexit.register(OtelTracer.shutdown)
        """
        shutdown_tracing()
        cls._initialized = False


# Made with Bob
