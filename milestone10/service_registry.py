"""Lease-based in-memory service registry for naming/discovery exercises."""

class ServiceRegistry:
    def __init__(self):
        self._services = {}

    def register(self, name, address, expires_at):
        if not name or not address:
            raise ValueError("name and address are required")
        self._services[name] = {"address": address, "expires_at": float(expires_at)}

    def resolve(self, name, now):
        entry = self._services.get(name)
        if entry is None or entry["expires_at"] <= now:
            self._services.pop(name, None)
            return None
        return entry["address"]

    def expire(self, now):
        expired = [name for name, entry in self._services.items()
                   if entry["expires_at"] <= now]
        for name in expired:
            del self._services[name]
        return expired
