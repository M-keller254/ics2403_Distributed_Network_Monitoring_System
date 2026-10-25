"""Run the milestone 7 transient-failure recovery example."""
from recovery import retry

attempts = []
def flaky_operation():
    attempts.append(len(attempts) + 1)
    if len(attempts) < 3:
        raise ConnectionError("simulated temporary collector outage")
    return {"status": "recovered", "attempt": len(attempts)}

result = retry(flaky_operation, attempts=4, sleep=lambda _: None)
print("Recovery result:", result)
print("Attempt count:", len(attempts))
