"""gRPC server (protobuf contract in monitor.proto) for the Milestone 11 benchmark. Also answers /health on PORT+1 over HTTP."""
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer

import grpc

from system.common.config import env_int
from system.rpc import monitor_pb2 as pb
from system.rpc import monitor_pb2_grpc as pbg

PORT = env_int("PORT", 7701)


class Monitor(pbg.MonitorServicer):
    def __init__(self):
        self.counter, self.readings, self.lock, self.seen = 0, 0, threading.Lock(), {}

    def GetSite(self, request, context):
        return pb.SiteState(site=request.site, cpu_pct=41.5, mem_pct=62.1, latency_ms=23.4, readings=1234)

    def PutReadings(self, request, context):
        with self.lock:
            self.readings += len(request.readings)
        return pb.Ack(accepted=len(request.readings))

    def Increment(self, request, context):
        with self.lock:
            if request.request_id and request.request_id in self.seen:      # at-most-once via request id
                return pb.IncResp(value=self.seen[request.request_id])
            self.counter += 1
            if request.request_id:
                self.seen[request.request_id] = self.counter
            return pb.IncResp(value=self.counter)

    def StreamReadings(self, request, context):
        for i in range(request.count):
            yield pb.Reading(record_id=i, site="NAI-01", cpu_pct=41.5, mem_pct=62.1, rx_mbps=120.5, tx_mbps=80.25, latency_ms=23.4,
                             jitter_ms=2.1, packet_loss_pct=0.05, active_users=321, is_anomaly=False)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), options=[("grpc.max_send_message_length", 64 << 20), ("grpc.max_receive_message_length", 64 << 20)])
    pbg.add_MonitorServicer_to_server(Monitor(), server)
    server.add_insecure_port(f"0.0.0.0:{PORT}")
    server.start()

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b'{"status":"ok"}')

        def log_message(self, *a):
            pass
    threading.Thread(target=HTTPServer(("0.0.0.0", PORT + 1), H).serve_forever, daemon=True).start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
