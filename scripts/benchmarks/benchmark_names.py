"""Shared benchmark-name normalization for performance-report tooling."""

DRIVER_METHOD_NAMES = {
    "thisDriver": "thisDriver",
    "clientV2": "clientV2",
    "aThisDriver": "thisDriver",
    "zClientV2": "clientV2",
}


def driver_name(benchmark_fqn: str) -> str:
    """Return the stable driver label encoded by a JMH benchmark method name."""
    method_name = benchmark_fqn.rsplit(".", 1)[-1]
    return DRIVER_METHOD_NAMES.get(method_name, method_name)
