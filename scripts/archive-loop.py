#!/usr/bin/env python3
"""Copy and verify a fixed snapshot, one server at a time. Re-run to resume."""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

child = None
stopping = False

def stop(signum, frame):
    global stopping
    stopping = True
    if child is not None and child.poll() is None:
        child.send_signal(signal.SIGTERM)

def atomic_json(path, data):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as out:
        json.dump(data, out, indent=2)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

def run(args):
    global child
    if stopping:
        return 130
    child = subprocess.Popen(args)  # Inherit the tmux terminal for the dashboard.
    while child.poll() is None:
        if stopping:
            try:
                return child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                return child.wait()
        time.sleep(0.2)
    return child.returncode

def main():
    os.umask(0o077)
    config_path = Path(sys.argv[1]).resolve()
    config = json.loads(config_path.read_text())
    state = config_path.parent
    lock = (state / "loop.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    identity = {key: config[key] for key in
                ("endpoint", "snapshot", "snapshot_guid", "destination", "mount_uuid")}
    failures = []
    for server in config["servers"]:
        if stopping:
            return 130
        if server in (".", "..") or "/" in server or not server:
            raise ValueError("invalid server name")
        mounted = subprocess.check_output(
            ["findmnt", "-n", "-o", "UUID", "--mountpoint", config["mount"]],
            text=True).strip()
        if mounted != config["mount_uuid"]:
            raise RuntimeError("archive filesystem is not mounted")
        directory = state / server
        directory.mkdir(exist_ok=True)
        done = directory / "done.json"
        expected = dict(identity, server=server)
        if done.exists():
            record = json.loads(done.read_text())
            if record["identity"] != expected:
                raise RuntimeError("completion marker belongs to a different archive")
            print(f"Skipping {server}: done", flush=True)
            continue
        attempt = time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
        report = directory / (attempt + ".verification.jsonl")
        dest = Path(config["destination"]) / server
        dest.mkdir(parents=True, exist_ok=True)
        common = [config["binary"], "", config["endpoint"], "--source", server,
                  "--directory", str(dest), "--ramlimit", str(config["ramlimit"])]
        if config.get("password_file"):
            common += ["--password-file", config["password_file"]]
        status = dict(identity=expected, attempt=attempt, report=str(report))
        for phase in ("copying", "verifying"):
            status.update(phase=phase, updated=time.time())
            atomic_json(state / "current.json", status)
            atomic_json(directory / "status.json", status)
            print(f"\n{server}: {phase} ({attempt})", flush=True)
            args = common.copy()
            if phase == "copying":
                args[1] = "client"
                args += ["--hardlinks", "--xattr", "--pfile", "64", "--pdir", "8",
                         "--queuesize", "1024", "--blocksize", str(config.get("blocksize",65536))]
                if config.get("resume_cache", False):
                    args += ["--resume-cache", str(directory / "reuse-hints.jsonl"),
                             "--resume-id", str(config["snapshot_guid"])]
                    if config.get("resume_position", False):
                        args += ["--resume-position"]
                args += config.get("client_args", [])
            else:
                args[1] = "verify"
                args += ["--report", str(report)]
            result = run(args)
            status[phase + "_exit_code"] = result
            if stopping or result != 0:
                status["phase"] = "interrupted" if stopping else phase + "-failed"
                break
        else:
            summary = None
            with report.open() as records:
                for line in records:
                    summary = json.loads(line)
            if not summary or summary.get("type") != "summary" or not summary.get("complete") or summary.get("errors", 0):
                raise RuntimeError("verification did not produce a successful summary")
            # Flush this completed archive before committing its durable DONE record.
            subprocess.run(["sync", "-f", str(dest)], check=True)
            status.update(phase="done", summary=summary, finished=time.time())
            atomic_json(done, status)
        atomic_json(directory / "status.json", status)
        atomic_json(state / "current.json", status)
        if stopping:
            return 130
        if status["phase"] != "done":
            failures.append(server)
            print(f"{server}: failed; will retry on next run", flush=True)
    print("Loop finished. Failed servers: " + (", ".join(failures) or "none"), flush=True)
    return bool(failures)

if __name__ == "__main__":
    sys.exit(main())
