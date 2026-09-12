import subprocess
import time
import sys
import shutil
import socket

host = "127.0.0.1"
port = 5432
timeout = 60

pg_isready_bin = shutil.which("pg_isready") or "/usr/lib/postgresql/18/bin/pg_isready"

start_time = time.time()
while time.time() - start_time < timeout:
    try:
        res = subprocess.run(
            [pg_isready_bin, "-h", host, "-p", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        if res.returncode == 0:
            print("==> PostgreSQL is ready and accepting connections!")
            sys.exit(0)
    except Exception:
        # Fallback to socket test if pg_isready is unavailable
        try:
            with socket.create_connection((host, port), timeout=1):
                print("==> PostgreSQL port is open!")
                sys.exit(0)
        except OSError:
            pass
    print("==> Waiting for PostgreSQL to be ready...")
    time.sleep(1)

print("==> PostgreSQL connection timeout.")
sys.exit(1)
