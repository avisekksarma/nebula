def main() -> None:
    import argparse
    import os

    import uvicorn

    parser = argparse.ArgumentParser(description="Run a Quasar KV node")
    parser.add_argument("--node-id", default="A")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--peers",
        default="",
        help="Other nodes as id=url pairs, e.g. B=http://127.0.0.1:8002,C=http://127.0.0.1:8003",
    )
    parser.add_argument(
        "--leader",
        default="A",
        help="Node ID of the manually configured leader",
    )
    args = parser.parse_args()

    os.environ["QUASAR_NODE_ID"] = args.node_id
    os.environ["QUASAR_PEERS"] = args.peers
    os.environ["QUASAR_LEADER"] = args.leader
    uvicorn.run("quasar.app:app", host=args.host, port=args.port, access_log=False)
