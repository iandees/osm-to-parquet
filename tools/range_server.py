"""Static file server with HTTP Range support that logs one line per request
with the byte range served, to count object-store style range reads."""
import sys
import os
from http.server import ThreadingHTTPServer
from RangeHTTPServer import RangeRequestHandler

class H(RangeRequestHandler):
    def log_message(self, fmt, *args):
        rng = self.headers.get("Range", "-") if hasattr(self, "headers") else "-"
        sys.stdout.write(f"{self.command} {self.path} {rng} {args[1] if len(args) > 1 else ''}\n"); sys.stdout.flush()
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*"); super().end_headers()

os.chdir(sys.argv[1])
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[2])), H).serve_forever()
