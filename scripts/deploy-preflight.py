#!/usr/bin/env python3
"""Validate rendered CellHive deployment topology (requires PyYAML)."""

import argparse
import json
import os
import subprocess
import sys

import yaml


def render(args):
    if args.target == "compose":
        cmd = ["docker", "compose", "-f", "deploy/compose/docker-compose.yml"]
        if args.profile:
            cmd += ["--profile", args.profile]
        raw = subprocess.check_output(cmd + ["config", "--format", "json"], text=True)
        services = json.loads(raw)["services"]
        agent = services["cell-agent"]["environment"]
        return {
            "agents": 1,
            "bucket": agent.get("CELLHIVE_BUCKET", ""),
            "targets": agent.get("CELLHIVE_DO_RUNTIMES", ""),
            "runtimes": {name: int(name == agent.get("CELLHIVE_DO_RUNTIMES", "").split(":")[0]) for name in ("do-runtime", "do-runtime-gated") if name in services},
            "profile": args.profile,
            "cell_url": services["user-runtime"]["environment"].get("CELLHIVE_CELL_URL", ""),
            "peer_url": "http://cell-agent:7001",
        }
    cmd = ["kubectl", "kustomize", args.path] if args.target == "k8s" else [
        "helm", "template", "cellhive", args.path, "--namespace", args.namespace,
        "--set", "existingSecret=cellhive-secrets",
    ]
    docs = [doc for doc in yaml.safe_load_all(subprocess.check_output(cmd, text=True)) if doc]
    config = next(doc["data"] for doc in docs if doc.get("kind") == "ConfigMap" and doc["metadata"]["name"].endswith("-config"))
    workloads = [doc for doc in docs if doc.get("kind") in ("Deployment", "StatefulSet")]
    agent = next(doc for doc in workloads if doc["metadata"]["name"].endswith("cell-agent"))
    env = agent["spec"]["template"]["spec"]["containers"][0].get("env", [])
    return {
        "agents": agent["spec"].get("replicas", 1),
        "bucket": config.get("CELLHIVE_BUCKET", ""),
        "targets": config.get("CELLHIVE_DO_RUNTIMES", ""),
        "runtimes": {doc["metadata"]["name"]: doc["spec"].get("replicas", 1) for doc in workloads if doc["metadata"]["name"].endswith(("do-runtime", "do-runtime-gated"))},
        "cell_url": config.get("CELLHIVE_CELL_URL", ""),
        "peer_url": next((entry.get("value", "") for entry in env if entry["name"] == "CELLHIVE_PEER_URL"), ""),
    }


def validate(state):
    errors = []
    if state["agents"] > 1 and not state["bucket"].startswith("s3://"):
        errors.append("multiple cell-agent PVCs require a shared S3 bucket")
    if not state["cell_url"] or not state["peer_url"]:
        errors.append("runtime cell URL or cell-agent peer URL missing")
    targets = [value.strip().removeprefix("http://").removeprefix("https://").split(":")[0] for value in state["targets"].split(",") if value.strip()]
    if not targets:
        errors.append("DO placement list empty")
    if state.get("profile") == "rpo0" and "do-runtime-gated" not in targets:
        errors.append("rpo0 profile requires CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788")
    for name, replicas in state["runtimes"].items():
        if replicas > 1:
            errors.append(f"{name}: multiple DO pods behind one DNS shard are unsafe")
        if replicas and not any(target == name or name.endswith("-" + target) or target.endswith("-" + name) for target in targets):
            errors.append(f"{name}: active DO workload absent from placement list")
    return errors
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=("compose", "k8s", "helm"))
    parser.add_argument("--path", default="")
    parser.add_argument("--profile", default="")
    parser.add_argument("--namespace", default="cellhive")
    args = parser.parse_args()
    args.path = args.path or ("deploy/helm/cellhive" if args.target == "helm" else "deploy/k8s/overlays/rpo0")
    try:
        state = render(args)
        errors = validate(state)
    except (OSError, subprocess.CalledProcessError, ValueError, StopIteration, KeyError) as exc:
        print(f"deploy preflight: {exc}", file=sys.stderr)
        return 1
    for error in errors:
        print(f"deploy preflight: {error}", file=sys.stderr)
    if errors:
        return 1
    print(f"deploy preflight: {args.target} topology OK (agent={state['agents']}, bucket={'S3' if state['bucket'] else 'filesystem'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
