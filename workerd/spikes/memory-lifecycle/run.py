"""Isolated named/anonymous/self-abort comparison using stock pinned workerd."""
import argparse
import json
import pathlib
import subprocess
import time
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--image", default="cellhive:runtime-baseline")
p.add_argument("--output", required=True, type=pathlib.Path)
p.add_argument("--count", default=1000, type=int)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
source = pathlib.Path(__file__).resolve().parent
version = subprocess.check_output(["docker", "run", "--rm", a.image, "workerd", "--version"], text=True)
(a.output / "version.txt").write_text(version)
for mode in ["named", "anonymous", "abort"]:
    name = f"cellhive-life-{mode}-{time.time_ns()}"
    records = []

    def record(value):
        records.append(value)
        print(json.dumps(value), flush=True)

    def sample(stage, count):
        result = subprocess.run(["docker", "exec", name, "sh", "-c",
                                 "cat /proc/1/status /sys/fs/cgroup/memory.current"],
                                capture_output=True, text=True)
        fields = [line for line in result.stdout.splitlines()
                  if line.startswith(("VmRSS:", "VmHWM:")) or line.isdigit()]
        record({"mode": mode, "stage": stage, "count": count, "memory": fields})

    subprocess.run(["docker", "run", "-d", "--name", name, "--memory=512m", "--memory-swap=512m", "--cpus=2",
                    "-p", "127.0.0.1::8099", "-v", f"{source}:/probe:ro", "--entrypoint", "/usr/local/bin/workerd",
                    a.image, "serve", "--experimental", "/probe/probe.capnp"], check=True, capture_output=True)
    try:
        port = subprocess.check_output(["docker", "port", name, "8099"], text=True).strip().split(":")[-1]
        base = "http://127.0.0.1:" + port + "/"

        def get(query):
            with urllib.request.urlopen(base + "?" + query, timeout=20) as response:
                return json.load(response)

        for attempt in range(100):
            try:
                urllib.request.urlopen(base + "?health=1", timeout=1).close()
                break
            except Exception:
                time.sleep(.1)
        first, second = get(f"mode={mode}&id=same"), get(f"mode={mode}&id=same")
        record({"mode": mode, "stage": "semantics", "first": first, "second": second})
        assert first == {"counter": 1, "loads": 1}
        assert second == ({"counter": 2, "loads": 1} if mode == "named" else {"counter": 1, "loads": 2})
        sample("baseline", 0)
        for i in range(a.count):
            value = get(f"mode={mode}&id=v{i}&bytes=1048576")
            assert value["counter"] == 1
            if i % 100 == 99:
                sample("load", i+1)
        sample("loaded", a.count)
        time.sleep(15)
        sample("idle15s", a.count)
    except Exception as error:
        record({"mode": mode, "stage": "failure", "error": str(error)})
    finally:
        inspect = json.loads(subprocess.check_output(["docker", "inspect", name]))[0]
        record({"mode": mode, "stage": "state", "state": inspect["State"], "image": inspect["Image"]})
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
        (a.output / f"{mode}.log").write_text(logs.stdout + logs.stderr)
        (a.output / f"{mode}.json").write_text(json.dumps(records, indent=2) + "\n")
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
