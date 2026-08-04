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
OpenTelemetry Metrics Utility

This module provides a centralized metrics configuration for collecting
performance and operational metrics across the data pipeline services.

Features:
- OTLP gRPC exporter for sending metrics to OpenTelemetry Collector
- Resource attributes for service identification
- Periodic export reader for regular metric export
- Support for counters, histograms, and gauges

Usage:
    from utils.otel_metrics import get_meter, create_counter, create_histogram
    
    meter = get_meter()
    messages_counter = create_counter("messages_processed", "Number of messages processed")
    messages_counter.add(1, {"status": "success"})
"""

import os
from typing import Optional, Dict, Any

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

# Global meter provider instance
_meter_provider: Optional[MeterProvider] = None


def setup_metrics(
    service_name: Optional[str] = None,
    service_version: Optional[str] = None,
    otlp_endpoint: Optional[str] = None,
    environment: Optional[str] = None,
    export_interval_millis: int = 60000,  # 60 seconds
) -> MeterProvider:
    """
    Initialize OpenTelemetry metrics with OTLP exporter.
    
    This function should be called once at application startup.
    
    Args:
        service_name: Name of the service (defaults to SERVICE_NAME env var)
        service_version: Version of the service (defaults to SERVICE_VERSION env var)
        otlp_endpoint: OTLP collector endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var)
        environment: Deployment environment (defaults to ENVIRONMENT env var)
        export_interval_millis: How often to export metrics in milliseconds
    
    Returns:
        MeterProvider: Configured meter provider
    
    Example:
        setup_metrics(
            service_name="producer-schema-mapper",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            environment="development"
        )
    """
    global _meter_provider
    
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
    
    # Configure OTLP exporter
    otlp_exporter = OTLPMetricExporter(
        endpoint=otlp_endpoint,
        insecure=os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "true").lower() == "true",
    )
    
    # Create periodic exporting metric reader
    metric_reader = PeriodicExportingMetricReader(
        exporter=otlp_exporter,
        export_interval_millis=export_interval_millis,
    )
    
    # Create meter provider
    _meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    
    # Set as global meter provider
    metrics.set_meter_provider(_meter_provider)
    
    return _meter_provider


def get_meter(name: Optional[str] = None) -> metrics.Meter:
    """
    Get a meter instance.
    
    If metrics haven't been set up, this will initialize them with default settings.
    
    Args:
        name: Name for the meter (defaults to calling module name)
    
    Returns:
        Meter: OpenTelemetry meter instance
    
    Example:
        meter = get_meter(__name__)
        counter = meter.create_counter("requests_total")
        counter.add(1)
    """
    global _meter_provider
    
    # Initialize metrics if not already done
    if _meter_provider is None:
        setup_metrics()
    
    # Get meter name from parameter or calling module
    meter_name = name or __name__
    
    return metrics.get_meter(meter_name)


def create_counter(
    name: str,
    description: str = "",
    unit: str = "1",
) -> metrics.Counter:
    """
    Create a counter metric.
    
    Counters are monotonically increasing values (e.g., total requests, errors).
    
    Args:
        name: Metric name (e.g., "messages_processed_total")
        description: Human-readable description
        unit: Unit of measurement (e.g., "1", "ms", "bytes")
    
    Returns:
        Counter: Counter metric instrument
    
    Example:
        messages_counter = create_counter(
            "messages_processed_total",
            "Total number of messages processed",
            "1"
        )
        messages_counter.add(1, {"status": "success", "topic": "my-topic"})
    """
    meter = get_meter()
    return meter.create_counter(
        name=name,
        description=description,
        unit=unit,
    )


def create_histogram(
    name: str,
    description: str = "",
    unit: str = "ms",
) -> metrics.Histogram:
    """
    Create a histogram metric.
    
    Histograms record distributions of values (e.g., request duration, message size).
    
    Args:
        name: Metric name (e.g., "message_processing_duration")
        description: Human-readable description
        unit: Unit of measurement (e.g., "ms", "bytes", "1")
    
    Returns:
        Histogram: Histogram metric instrument
    
    Example:
        duration_histogram = create_histogram(
            "message_processing_duration",
            "Time to process a message",
            "ms"
        )
        duration_histogram.record(45.2, {"operation": "validate"})
    """
    meter = get_meter()
    return meter.create_histogram(
        name=name,
        description=description,
        unit=unit,
    )


def create_up_down_counter(
    name: str,
    description: str = "",
    unit: str = "1",
) -> metrics.UpDownCounter:
    """
    Create an up-down counter metric.
    
    Up-down counters can increase or decrease (e.g., active connections, queue size).
    
    Args:
        name: Metric name (e.g., "active_connections")
        description: Human-readable description
        unit: Unit of measurement
    
    Returns:
        UpDownCounter: Up-down counter metric instrument
    
    Example:
        active_tasks = create_up_down_counter(
            "active_tasks",
            "Number of currently active tasks",
            "1"
        )
        active_tasks.add(1)  # Task started
        active_tasks.add(-1)  # Task completed
    """
    meter = get_meter()
    return meter.create_up_down_counter(
        name=name,
        description=description,
        unit=unit,
    )


def shutdown_metrics():
    """
    Shutdown metrics and flush any pending data.
    
    This should be called before application exit to ensure all metrics are exported.
    
    Example:
        import atexit
        atexit.register(shutdown_metrics)
    """
    global _meter_provider
    
    if _meter_provider is not None:
        _meter_provider.shutdown()
        _meter_provider = None


# ============================================================================
# Class-based API (Wrapper for backward compatibility)
# ============================================================================

class OtelMetrics:
    """
    OpenTelemetry Metrics Factory (Class-based API).
    
    This class provides a class-based interface that wraps the function-based
    API above. It maintains compatibility with code that expects a class-based
    interface as specified in the implementation guide.
    
    Usage:
        from utils.otel_metrics import OtelMetrics
        
        # Initialize at startup
        meter = OtelMetrics.initialize(
            service_name="my-service",
            service_version="1.0.0"
        )
        
        # Create metrics
        counter = meter.create_counter("requests_total")
        counter.add(1, {"status": "success"})
    """
    
    _initialized: bool = False
    
    @classmethod
    def initialize(
        cls,
        service_name: Optional[str] = None,
        service_version: Optional[str] = None,
        otlp_endpoint: Optional[str] = None,
        environment: Optional[str] = None,
        export_interval_millis: int = 60000,
    ) -> metrics.Meter:
        """
        Initialize OpenTelemetry metrics.
        
        This is a convenience method that wraps setup_metrics() and returns
        a meter instance.
        
        Args:
            service_name: Name of the service (defaults to SERVICE_NAME env var)
            service_version: Version of the service (defaults to SERVICE_VERSION env var)
            otlp_endpoint: OTLP collector endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var)
            environment: Deployment environment (defaults to ENVIRONMENT env var)
            export_interval_millis: How often to export metrics in milliseconds
        
        Returns:
            metrics.Meter: Configured meter instance
        
        Example:
            meter = OtelMetrics.initialize(
                service_name="producer-schema-mapper",
                service_version="1.0.0",
                otlp_endpoint="http://otel-collector:4317",
                environment="development"
            )
        """
        if not cls._initialized:
            setup_metrics(
                service_name=service_name,
                service_version=service_version,
                otlp_endpoint=otlp_endpoint,
                environment=environment,
                export_interval_millis=export_interval_millis,
            )
            cls._initialized = True
        
        return metrics.get_meter(__name__)
    
    @classmethod
    def get_meter(cls, name: Optional[str] = None) -> metrics.Meter:
        """
        Get a meter instance.
        
        If metrics haven't been initialized, this will initialize them with
        default settings.
        
        Args:
            name: Name for the meter (defaults to __name__)
        
        Returns:
            metrics.Meter: OpenTelemetry meter instance
        
        Example:
            meter = OtelMetrics.get_meter(__name__)
            counter = meter.create_counter("requests_total")
            counter.add(1)
        """
        if not cls._initialized:
            cls.initialize()
        
        return get_meter(name)
    
    @classmethod
    def create_counter(
        cls,
        name: str,
        description: str = "",
        unit: str = "1",
    ) -> metrics.Counter:
        """
        Create a counter metric.
        
        This is a convenience method that wraps create_counter().
        
        Args:
            name: Metric name (e.g., "messages_processed_total")
            description: Human-readable description
            unit: Unit of measurement (e.g., "1", "ms", "bytes")
        
        Returns:
            Counter: Counter metric instrument
        
        Example:
            counter = OtelMetrics.create_counter(
                "messages_processed_total",
                "Total number of messages processed",
                "1"
            )
            counter.add(1, {"status": "success"})
        """
        if not cls._initialized:
            cls.initialize()
        
        return create_counter(name, description, unit)
    
    @classmethod
    def create_histogram(
        cls,
        name: str,
        description: str = "",
        unit: str = "ms",
    ) -> metrics.Histogram:
        """
        Create a histogram metric.
        
        This is a convenience method that wraps create_histogram().
        
        Args:
            name: Metric name (e.g., "message_processing_duration")
            description: Human-readable description
            unit: Unit of measurement (e.g., "ms", "bytes", "1")
        
        Returns:
            Histogram: Histogram metric instrument
        
        Example:
            histogram = OtelMetrics.create_histogram(
                "message_processing_duration",
                "Time to process a message",
                "ms"
            )
            histogram.record(45.2, {"operation": "validate"})
        """
        if not cls._initialized:
            cls.initialize()
        
        return create_histogram(name, description, unit)
    
    @classmethod
    def create_up_down_counter(
        cls,
        name: str,
        description: str = "",
        unit: str = "1",
    ) -> metrics.UpDownCounter:
        """
        Create an up-down counter metric.
        
        This is a convenience method that wraps create_up_down_counter().
        
        Args:
            name: Metric name (e.g., "active_connections")
            description: Human-readable description
            unit: Unit of measurement
        
        Returns:
            UpDownCounter: Up-down counter metric instrument
        
        Example:
            counter = OtelMetrics.create_up_down_counter(
                "active_tasks",
                "Number of currently active tasks",
                "1"
            )
            counter.add(1)  # Task started
            counter.add(-1)  # Task completed
        """
        if not cls._initialized:
            cls.initialize()
        
        return create_up_down_counter(name, description, unit)
    
    @classmethod
    def shutdown(cls):
        """
        Shutdown metrics and flush any pending data.
        
        This is a convenience method that wraps shutdown_metrics().
        
        Example:
            import atexit
            atexit.register(OtelMetrics.shutdown)
        """
        shutdown_metrics()
        cls._initialized = False


# Made with Bob
