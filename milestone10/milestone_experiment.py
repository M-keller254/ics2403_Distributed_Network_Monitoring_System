"""Show service registration, resolution, and lease expiration."""
from service_registry import ServiceRegistry

registry = ServiceRegistry()
registry.register("collector", "http://core-node:5000", expires_at=30)
print("At time 29:", registry.resolve("collector", now=29))
print("Expired at time 30:", registry.resolve("collector", now=30))
