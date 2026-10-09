"""Only endpoint/launch adaptation for the unchanged host-oriented M9 oracle."""
import os
import runpy
import socket
import socketserver
import subprocess
import sys
import threading
import select

class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        with socket.create_connection(self.server.target, timeout=5) as target:
            while True:
                ready, _, _ = select.select([self.request, target], [], [], 10)
                if not ready:
                    return
                for source in ready:
                    data = source.recv(65536)
                    if not data:
                        return
                    (target if source is self.request else self.request).sendall(data)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

servers = []
for port, host, remote in [(19090, 'prometheus', 9090), (13000, 'grafana', 3000), (18000, 'metrics-exporter', 8000)]:
    server = Server(('127.0.0.1', port), Forward)
    server.target = (host, remote)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    servers.append(server)

import scripts.verify_milestone9 as oracle
oracle.dotenv_values = lambda _: {'HOST_PROMETHEUS_PORT': '19090', 'HOST_GRAFANA_PORT': '13000', 'HOST_METRICS_PORT': '18000', 'GRAFANA_ADMIN_USER': 'admin', 'GRAFANA_ADMIN_PASSWORD': os.environ['GRAFANA_ADMIN_PASSWORD']}
original = subprocess.run
def invocation(command, *args, **kwargs):
    if isinstance(command, list) and 'oip-modernization-verify' in command:
        # Replace only the historical container launch with a fresh packaged process.
        index = command.index('python3')
        command = [sys.executable, *command[index + 1:]]
    return original(command, *args, **kwargs)
subprocess.run = invocation
try:
    oracle.main()
finally:
    subprocess.run = original
    for server in servers:
        server.shutdown()
        server.server_close()
