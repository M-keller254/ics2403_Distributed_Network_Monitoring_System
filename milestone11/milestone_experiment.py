"""Call a live collector using the milestone 11 HTTP RPC client."""
import sys
from rpc_client import CollectorClient

base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
client = CollectorClient(base_url)
print("Collector status:")
print(client.status())
