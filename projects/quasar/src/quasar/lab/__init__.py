"""Interactive lab: website + cluster orchestrator. Not part of the KV node."""


def main() -> None:
    import argparse
    import os

    import uvicorn

    parser = argparse.ArgumentParser(
        description="Run the Quasar interactive lab (3 nodes + website)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    os.environ["QUASAR_LAB_PORT"] = str(args.port)
    url = f"http://{args.host}:{args.port}"
    print(f"Quasar lab: {url}", flush=True)
    print("Starting nodes A, B, C on 8001–8003…", flush=True)
    uvicorn.run("quasar.lab.server:app", host=args.host, port=args.port, access_log=False)
