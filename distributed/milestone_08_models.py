"""Vector clocks for illustrating causal ordering in a distributed system."""

class VectorClock:
    def __init__(self, members, values=None):
        self.members = tuple(members)
        self.values = {member: 0 for member in self.members}
        if values:
            if set(values) != set(self.members):
                raise ValueError("clock members must match")
            self.values.update(values)

    def tick(self, member):
        self._check_member(member)
        self.values[member] += 1
        return self

    def merge(self, other):
        if self.members != other.members:
            raise ValueError("clock membership differs")
        for member in self.members:
            self.values[member] = max(self.values[member], other.values[member])
        return self

    def relation(self, other):
        """Return before, after, equal, or concurrent."""
        if self.members != other.members:
            raise ValueError("clock membership differs")
        le = all(self.values[m] <= other.values[m] for m in self.members)
        ge = all(self.values[m] >= other.values[m] for m in self.members)
        if le and ge:
            return "equal"
        if le:
            return "before"
        if ge:
            return "after"
        return "concurrent"

    def _check_member(self, member):
        if member not in self.values:
            raise KeyError(member)
