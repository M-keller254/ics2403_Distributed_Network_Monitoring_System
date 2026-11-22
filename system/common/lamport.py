"""Logical clocks (Milestone 4 / 8): Lamport scalar clock and vector clock."""
import threading


class LamportClock:
    """tick() on local/send events, update(received) on receive: max(local, received) + 1.
    If event A caused B then clock(A) < clock(B); the converse is not guaranteed."""

    def __init__(self):
        self._t = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._t += 1
            return self._t

    def update(self, received):
        with self._lock:
            self._t = max(self._t, int(received)) + 1
            return self._t

    @property
    def value(self):
        with self._lock:
            return self._t


class VectorClock:
    """Vector clock over a fixed set of members. Unlike Lamport, it can tell
    'happened-before' from 'concurrent' (Milestone 8, event ordering in the models)."""

    def __init__(self, members, values=None):
        self.members = tuple(members)
        self.v = {m: 0 for m in self.members}
        if values:
            self.v.update({k: int(x) for k, x in values.items() if k in self.v})

    def tick(self, member):
        self.v[member] += 1
        return self

    def merge(self, other):
        for m in self.members:
            self.v[m] = max(self.v[m], other.v.get(m, 0))
        return self

    def compare(self, other):
        """'before' | 'after' | 'equal' | 'concurrent'."""
        le = all(self.v[m] <= other.v.get(m, 0) for m in self.members)
        ge = all(self.v[m] >= other.v.get(m, 0) for m in self.members)
        if le and ge:
            return "equal"
        if le:
            return "before"
        if ge:
            return "after"
        return "concurrent"

    def as_dict(self):
        return dict(self.v)
