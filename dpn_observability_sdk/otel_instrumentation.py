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
OpenTelemetry Instrumentation Utility

This module provides convenient decorators and helpers for instrumenting
Python functions with OpenTelemetry tracing and metrics.

Features:
- @traced decorator for automatic span creation
- @timed_metric decorator for duration tracking
- @counter_metric decorator for event counting
- Automatic error recording and status setting
- Context propagation support

Usage:
    from dpn_observability_sdk.otel_instrumentation import setup_telemetry, traced, timed_metric
    
    # Initialize at startup
    setup_telemetry(
        service_name="my-service",
        otlp_endpoint="http://otel-collector:4317"
    )
    
    # Use decorators
    @traced(span_name="process_data")
    @timed_metric("data_processing_duration")
    def process_data(data):
        # Your code here
        return result
"""

import functools
import time
from typing import Optional, Callable, Any, Dict
import os

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from dpn_observability_sdk.otel_tracer import setup_tracing, get_tracer, shutdown_tracing
from dpn_observability_sdk.otel_metrics import setup_metrics, create_counter, create_histogram, shutdown_metrics


def setup_telemetry(
    service_name: Optional[str] = None,
    service_version: Optional[str] = None,
    otlp_endpoint: Optional[str] = None,
    environment: Optional[str] = None,
) -> None:
    """
    Initialize both tracing and metrics with a single call.
    
    This is a convenience function that sets up both tracing and metrics
    with the same configuration. Call this once at application startup.
    
    Args:
        service_name: Name of the service
        service_version: Version of the service
        otlp_endpoint: OTLP collector endpoint
        environment: Deployment environment
    
    Example:
        from dpn_observability_sdk.otel_instrumentation import setup_telemetry
        
        setup_telemetry(
            service_name="producer-schema-mapper",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            environment="development"
        )
    """
    # Setup tracing
    setup_tracing(
        service_name=service_name,
        service_version=service_version,
        otlp_endpoint=otlp_endpoint,
        environment=environment,
    )
    
    # Setup metrics
    setup_metrics(
        service_name=service_name,
        service_version=service_version,
        otlp_endpoint=otlp_endpoint,
        environment=environment,
    )


def shutdown_telemetry() -> None:
    """
    Shutdown both tracing and metrics.
    
    Call this before application exit to ensure all telemetry is flushed.
    
    Example:
        import atexit
        from dpn_observability_sdk.otel_instrumentation import shutdown_telemetry
        
        atexit.register(shutdown_telemetry)
    """
    shutdown_tracing()
    shutdown_metrics()


def traced(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
) -> Callable:
    """
    Decorator to automatically create a span for a function.
    
    This decorator wraps a function with OpenTelemetry tracing, automatically
    creating a span, recording exceptions, and setting the span status.
    
    Args:
        span_name: Name for the span (defaults to function name)
        attributes: Static attributes to add to the span
    
    Returns:
        Decorated function
    
    Example:
        @traced(span_name="validate_message")
        def validate(self, message):
            # Your code here
            return is_valid
        
        # With attributes
        @traced(span_name="process", attributes={"component": "validator"})
        def process(data):
            return result
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get tracer
            tracer = get_tracer(__name__)
            
            # Determine span name
            name = span_name or func.__name__
            
            # Start span
            with tracer.start_as_current_span(name) as span:
                # Add static attributes if provided
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Add function metadata
                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.namespace", func.__module__)
                
                try:
                    # Execute function
                    result = func(*args, **kwargs)
                    
                    # Set success status
                    span.set_status(Status(StatusCode.OK))
                    
                    return result
                    
                except Exception as exc:
                    # Record exception
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    
                    # Re-raise exception
                    raise
        
        return wrapper
    return decorator


def timed_metric(
    metric_name: str,
    description: str = "",
    unit: str = "ms",
    attributes: Optional[Dict[str, Any]] = None,
) -> Callable:
    """
    Decorator to automatically record function execution duration as a metric.
    
    This decorator measures how long a function takes to execute and records
    it as a histogram metric.
    
    Args:
        metric_name: Name of the metric
        description: Human-readable description
        unit: Unit of measurement (default: "ms")
        attributes: Static attributes to add to the metric
    
    Returns:
        Decorated function
    
    Example:
        @timed_metric("message_processing_duration", "Time to process message")
        def process_message(self, msg):
            # Your code here
            return result
        
        # With attributes
        @timed_metric(
            "validation_duration",
            "Schema validation time",
            attributes={"schema": "eqbd"}
        )
        def validate(data):
            return is_valid
    """
    # Create histogram metric
    histogram = create_histogram(metric_name, description, unit)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Record start time
            start_time = time.perf_counter()
            
            try:
                # Execute function
                result = func(*args, **kwargs)
                
                # Calculate duration in milliseconds
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                # Prepare attributes
                metric_attributes = attributes.copy() if attributes else {}
                metric_attributes["function"] = func.__name__
                metric_attributes["status"] = "success"
                
                # Record metric
                histogram.record(duration_ms, metric_attributes)
                
                return result
                
            except Exception as exc:
                # Calculate duration even on error
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                # Prepare attributes with error info
                metric_attributes = attributes.copy() if attributes else {}
                metric_attributes["function"] = func.__name__
                metric_attributes["status"] = "error"
                metric_attributes["error_type"] = type(exc).__name__
                
                # Record metric
                histogram.record(duration_ms, metric_attributes)
                
                # Re-raise exception
                raise
        
        return wrapper
    return decorator


def counter_metric(
    metric_name: str,
    description: str = "",
    unit: str = "1",
    attributes: Optional[Dict[str, Any]] = None,
    increment: int = 1,
) -> Callable:
    """
    Decorator to automatically increment a counter when a function is called.
    
    This decorator increments a counter metric each time the function is called,
    useful for tracking invocations, events, or processed items.
    
    Args:
        metric_name: Name of the metric
        description: Human-readable description
        unit: Unit of measurement (default: "1")
        attributes: Static attributes to add to the metric
        increment: Amount to increment (default: 1)
    
    Returns:
        Decorated function
    
    Example:
        @counter_metric("messages_processed_total", "Total messages processed")
        def process_message(self, msg):
            # Your code here
            return result
        
        # With attributes
        @counter_metric(
            "validations_total",
            "Total validations performed",
            attributes={"validator": "schema"}
        )
        def validate(data):
            return is_valid
    """
    # Create counter metric
    counter = create_counter(metric_name, description, unit)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Execute function
                result = func(*args, **kwargs)
                
                # Prepare attributes
                metric_attributes = attributes.copy() if attributes else {}
                metric_attributes["function"] = func.__name__
                metric_attributes["status"] = "success"
                
                # Increment counter
                counter.add(increment, metric_attributes)
                
                return result
                
            except Exception as exc:
                # Prepare attributes with error info
                metric_attributes = attributes.copy() if attributes else {}
                metric_attributes["function"] = func.__name__
                metric_attributes["status"] = "error"
                metric_attributes["error_type"] = type(exc).__name__
                
                # Increment counter (to track errors too)
                counter.add(increment, metric_attributes)
                
                # Re-raise exception
                raise
        
        return wrapper
    return decorator


def record_exception_metric(exc: Exception, attributes: Optional[Dict[str, Any]] = None) -> None:
    """
    Record an exception as a metric.
    
    This is a helper function to manually record exceptions as metrics,
    useful when you want to track specific error conditions.
    
    Args:
        exc: The exception to record
        attributes: Additional attributes for the metric
    
    Example:
        try:
            risky_operation()
        except ValueError as e:
            record_exception_metric(e, {"operation": "validation"})
            raise
    """
    error_counter = create_counter(
        "errors_total",
        "Total number of errors",
        "1"
    )
    
    metric_attributes = attributes.copy() if attributes else {}
    metric_attributes["error_type"] = type(exc).__name__
    metric_attributes["error_message"] = str(exc)
    
    error_counter.add(1, metric_attributes)


# Made with Bob
