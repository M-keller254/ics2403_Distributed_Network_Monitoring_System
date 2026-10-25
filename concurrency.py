"""Concurrency concepts: consistent lock order avoids circular wait."""
from threading import Lock

class OrderedLockPair:
    def __init__(self):
        self.first, self.second = Lock(), Lock()

    def execute(self, operation):
        with self.first:
            with self.second:
                return operation()

def has_wait_cycle(wait_edges):
    """Return whether a directed wait-for graph contains a cycle."""
    visiting, visited = set(), set()
    def visit(node):
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(target) for target in wait_edges.get(node, [])):
            return True
        visiting.remove(node)
        visited.add(node)
        return False
    return any(visit(node) for node in wait_edges)
