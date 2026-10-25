"""Small deterministic bully-election simulation for classroom discussion."""

def elect(available_node_ids):
    """Bully rule: highest available numeric node id wins; empty set has no leader."""
    nodes = sorted(available_node_ids, key=lambda item: int(item.rsplit("-", 1)[-1]))
    return nodes[-1] if nodes else None
