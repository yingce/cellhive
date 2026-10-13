#!/usr/bin/env python3
"""Run opt-in real-workerd memory probes; persist logs and Docker OOM evidence."""
import argparse
import json
import pathlib
import subprocess
import time


def save_container(name, output):
    log = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    (output / (name + ".log")).write_text(log.stdout + log.stderr)
    state = json.loads(subprocess.check_output(["docker", "inspect", name]))[0]
    evidence = {"name": name, "state": state["State"],
                "image": state["Image"], "args": state["Args"],
                "env": state["Config"]["Env"],
                "memory_limit": state["HostConfig"]["Memory"],
                "memory_swap": state["HostConfig"]["MemorySwap"],
                "ulimits": state["HostConfig"]["Ulimits"]}
    (output / (name + ".json")).write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"{name}: OOMKilled={state['State']['OOMKilled']} exit={state['State']['ExitCode']}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", required=True, type=pathlib.Path)
    p.add_argument("--collect", nargs="+", help="collect existing probe containers without rerunning")
    p.add_argument("--mode", choices=["workers", "versions", "same-sha", "hot", "assets", "do-facets", "do-workers"], default="workers")
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--bytes", type=int, default=1048576)
    p.add_argument("--memory", default="2g")
    p.add_argument("--builder", default="golang:1.27-bookworm")
    p.add_argument("--runtime-image", default="cellhive:runtime-baseline")
    a = p.parse_args()
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if a.collect:
        for name in a.collect:
            save_container(name, output)
        return 0
    if a.count <= 0 or a.bytes <= 0:
        p.error("count and bytes must be positive")
    repo = pathlib.Path(__file__).resolve().parent.parent
    kind = "doruntime" if a.mode.startswith("do-") else "userruntime"
    test = "TestFacetMemoryPressure" if kind == "doruntime" else "TestDynamicLoadMemoryPressure"
    tags = "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"
    subprocess.run(["docker", "run", "--rm", "-v", f"{repo}:/src:ro", "-v", f"{output}:/results", "-w", "/src", a.builder,
                    "go", "test", "-tags", tags, "-c", "-o", "/results/probe.test", f"./internal/{kind}"], check=True)
    version = subprocess.check_output(["docker", "run", "--rm", a.runtime_image, "workerd", "--version"], text=True)
    (output / "runtime-version.txt").write_text(version)
    name = f"cellhive-memory-{a.mode}-{time.time_ns()}"
    cmd = ["docker", "run", "--name", name, "--ulimit", "nofile=65536:65536", "--memory", a.memory, "--memory-swap", a.memory, "--cpus", "2",
           "-v", f"{repo}:/src:ro", "-v", f"{output}/probe.test:/bench:ro", "-w", f"/src/internal/{kind}",
           "-e", "CELLHIVE_MEMORY_STRESS=1", "-e", f"STRESS_MODE={a.mode}", "-e", f"STRESS_COUNT={a.count}", "-e", f"STRESS_BYTES={a.bytes}",
           "--entrypoint", "/bench", a.runtime_image, "-test.run", f"^{test}$", "-test.v", "-test.timeout=10m"]
    try:
        result = subprocess.run(cmd)
        save_container(name, output)
        return result.returncode
    finally:
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
