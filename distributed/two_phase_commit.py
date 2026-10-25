"""In-memory 2PC teaching simulation. It is not durable or crash-safe."""

def run_transaction(participants, fail_prepare=None):
    prepared = []
    for participant in participants:
        if participant == fail_prepare:
            return {"decision": "abort", "prepared": prepared, "committed": []}
        prepared.append(participant)
    return {"decision": "commit", "prepared": prepared, "committed": list(prepared)}
