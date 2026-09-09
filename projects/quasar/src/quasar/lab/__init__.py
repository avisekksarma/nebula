"""Quasar lab CLI. Starts the website and three real Quasar nodes."""


def main() -> None:
    import argparse
    import os

    import uvicorn

    parser = argparse.ArgumentParser(
        description="Quasar lab — visual debugger over a live 3-node cluster"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    os.environ["QUASAR_LAB_PORT"] = str(args.port)
    print(f"Quasar lab: http://{args.host}:{args.port}", flush=True)
    print("Starting real nodes A, B, C on 8001–8003…", flush=True)
    uvicorn.run("quasar.lab.server:app", host=args.host, port=args.port, access_log=False)
