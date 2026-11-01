from prometheus_client import Counter, Histogram, Gauge

REQUEST_COUNT = Counter(
    "gateway_requests_total",
    "Total incoming requests handled by the gateway",
    ["endpoint", "status"]
)

REQUEST_LATENCY = Histogram(
    "gateway_request_duration_seconds",
    "Request latency in seconds",
    ["route", "cached"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

CACHE_LOOKUPS = Counter(
    "gateway_cache_lookups_total",
    "Semantic cache lookup results",
    ["result"]  # "hit", "miss", "error"
)

ROUTING_DECISIONS = Counter(
    "gateway_routing_decisions_total",
    "Backend chosen by router",
    ["backend", "router_version", "reason"]
)

QUEUE_DEPTH = Gauge(
    "gateway_queue_depth",
    "Current active queue depth per backend",
    ["backend"]
)

CIRCUIT_BREAKER_STATE = Gauge(
    "gateway_circuit_breaker_state",
    "State of circuit breaker (0: closed, 1: half-open, 2: open)",
    ["backend"]
)

BACKEND_FAILURES = Counter(
    "gateway_backend_failures_total",
    "Backend failures categorized by type",
    ["backend", "failure_type"]
)

