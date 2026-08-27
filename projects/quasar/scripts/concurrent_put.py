"""Fire many concurrent PUTs at one key, then GET it. Server must already be running."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

BASE = "http://127.0.0.1:8000"
KEY_URL = f"{BASE}/kv/x"
N = 200
WORKERS = 32


def put(i: int) -> tuple[int, int, str]:
    r = httpx.put(KEY_URL, json={"value": str(i)})
    return i, r.status_code, r.json().get("value", "")


with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = [pool.submit(put, i) for i in range(N)]
    results = [f.result() for f in as_completed(futures)]

ok = sum(1 for _, status, _ in results if status == 200)
print(f"PUTs sent: {N}")
print(f"PUTs 200:  {ok}")

got = httpx.get(KEY_URL)
print(f"GET /kv/x -> {got.status_code} {got.json()}")
