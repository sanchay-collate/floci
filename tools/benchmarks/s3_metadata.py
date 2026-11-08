#!/usr/bin/env python3
"""Measure cold S3 metadata startup using synthetic data and an isolated container."""

import argparse
import http.client
import json
import sqlite3
import subprocess
import tempfile
import time
import uuid
import xml.etree.ElementTree as ElementTree
from pathlib import Path

ACCOUNT = "000000000000"
BUCKET = "metadata-benchmark"


def docker(*args, check=True, timeout=30):
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check, timeout=timeout
    )


def write_fixture(directory, entries, metadata_bytes):
    """Stream the snapshot so fixture creation does not itself retain the index."""
    if entries < 1 or metadata_bytes < 0:
        raise ValueError("entries must be positive and metadata_bytes nonnegative")
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o777)
    bucket = {
        "name": BUCKET,
        "creationDate": "2020-01-01T00:00:00Z",
        "region": "us-west-2",
        "versioningStatus": "Enabled",
        "tags": {},
    }
    (directory / "s3-buckets.json").write_text(
        json.dumps({f"{ACCOUNT}/{BUCKET}": bucket}), encoding="utf-8"
    )
    snapshot = directory / "s3-objects.json"
    with snapshot.open("w", encoding="utf-8") as output:
        output.write("{")
        for row in range(entries):
            number = row // 2
            key = f"logs/object-{number:010d}.json"
            version = str(uuid.UUID(int=number + 1))
            stored_key = f"{ACCOUNT}/{BUCKET}/{key}"
            if row % 2:
                stored_key += "#v#" + version
            value = {
                "bucketName": BUCKET,
                "key": key,
                "versionId": version,
                "ownerAccountId": ACCOUNT,
                "contentType": "application/json",
                "size": 64,
                "lastModified": "2020-01-01T00:00:00Z",
                "etag": '"00000000000000000000000000000000"',
                "storageClass": "STANDARD",
                "metadata": {"synthetic-padding": "x" * metadata_bytes},
                "tags": {},
                "parts": [],
                "checksum": {},
                "latest": True,
                "deleteMarker": False,
            }
            if row:
                output.write(",\n")
            output.write(json.dumps(stored_key) + ":" + json.dumps(value))
        output.write("}\n")
    return snapshot.stat().st_size


def head_bucket(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        connection.request("HEAD", "/" + BUCKET)
        return connection.getresponse().status
    except (OSError, http.client.HTTPException):
        return None
    finally:
        connection.close()


def memory_sample(container):
    result = docker(
        "exec",
        container,
        "sh",
        "-c",
        "cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak /proc/1/status",
        check=False,
    )
    lines = result.stdout.splitlines()
    if result.returncode or len(lines) < 2:
        return {}
    sample = {"cgroup_current_bytes": int(lines[0]), "cgroup_peak_bytes": int(lines[1])}
    for line in lines[2:]:
        for label, field in [
            ("VmRSS:", "process_rss_bytes"),
            ("VmHWM:", "process_peak_rss_bytes"),
        ]:
            if line.startswith(label):
                sample[field] = int(line.split()[1]) * 1024
    return sample


def exercise_index(port):
    """Bounded listings plus committed writes on the large synthetic index."""
    timings = {}
    operations = [
        ("objects", "GET", "/" + BUCKET + "?list-type=2&max-keys=1000", None),
        ("versions", "GET", "/" + BUCKET + "?versions&max-keys=1000", None),
        (
            "delimiter",
            "GET",
            "/" + BUCKET + "?list-type=2&delimiter=/&max-keys=1",
            None,
        ),
    ]
    for name, method, path, body in operations:
        started = time.monotonic()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.request(method, path, body)
            response = connection.getresponse()
            payload = response.read()
            if response.status != 200:
                raise RuntimeError(
                    f"{name} returned {response.status}: {payload[:300]!r}"
                )
            root = ElementTree.fromstring(payload)
            rows = len(root.findall("{*}Contents")) + len(root.findall("{*}Version"))
            timings[name] = {
                "seconds": round(time.monotonic() - started, 3),
                "rows": rows,
            }
        finally:
            connection.close()
    started = time.monotonic()
    for number in range(32):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.request(
                "PUT", "/" + BUCKET + f"/benchmark-writes/{number}", b"synthetic data"
            )
            response = connection.getresponse()
            response.read()
            if response.status != 200:
                raise RuntimeError(f"PUT returned {response.status}")
        finally:
            connection.close()
    timings["puts"] = {"seconds": round(time.monotonic() - started, 3), "requests": 32}
    return timings


def benchmark(args):
    # A missing image is an error, rather than an unexpected pull during a timed run.
    image = json.loads(docker("image", "inspect", args.image).stdout)[0]
    result = {
        "image": args.image,
        "image_id": image["Id"],
        "metadata_entries": args.entries,
        "metadata_padding_bytes": args.metadata_bytes,
        "memory_limit": args.memory,
        "object_index_layout": args.object_index_layout,
        "restarts": [],
        "cpus": args.cpus,
        "ready": False,
        "samples": [],
    }
    with tempfile.TemporaryDirectory(prefix="floci-metadata-benchmark-") as temp:
        data = Path(temp) / "data"
        result["snapshot_bytes"] = write_fixture(
            data, args.entries, args.metadata_bytes
        )
        command = [
            "create",
            "--label",
            "io.floci.metadata-benchmark=true",
            "--memory",
            args.memory,
            "--cpus",
            str(args.cpus),
            "--publish",
            "127.0.0.1::4566",
            "--mount",
            f"type=bind,source={data},target=/app/data",
            "--env",
            "FLOCI_STORAGE_MODE=memory",
            "--env",
            "FLOCI_STORAGE_SERVICES_S3_MODE=persistent",
            "--env",
            "FLOCI_SERVICES_S3_ENFORCE_AUTH=false",
            "--env",
            "FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=false",
            "--env",
            "FLOCI_SERVICES_UI_ENABLED=false",
        ]
        command += [
            "--env",
            "FLOCI_STORAGE_SERVICES_S3_OBJECT_INDEX_LAYOUT=" + args.object_index_layout,
        ]
        if args.entrypoint:
            command += [
                "--mount",
                f"type=bind,source={args.entrypoint.resolve()},target=/benchmark-entrypoint.sh,readonly",
                "--entrypoint",
                "/benchmark-entrypoint.sh",
            ]
        container = docker(*command, args.image).stdout.strip()
        try:
            for run in range(args.restarts + 1):
                current = result if run == 0 else {"ready": False, "samples": []}
                if run:
                    result["restarts"].append(current)
                started = time.monotonic()
                docker("start", container)
                port = int(
                    json.loads(docker("inspect", container).stdout)[0][
                        "NetworkSettings"
                    ]["Ports"]["4566/tcp"][0]["HostPort"]
                )
                while time.monotonic() - started < args.timeout:
                    state = json.loads(docker("inspect", container).stdout)[0]["State"]
                    current["state"] = state
                    if not state["Running"]:
                        break
                    sample = memory_sample(container)
                    sample["elapsed_seconds"] = round(time.monotonic() - started, 3)
                    sample["docker_health"] = state.get("Health", {}).get("Status")
                    current["samples"].append(sample)
                    if head_bucket(port) == 200:
                        current["ready"] = True
                        current["s3_ready_seconds"] = round(
                            time.monotonic() - started, 3
                        )
                        current["ready_memory"] = memory_sample(container)
                        break
                    time.sleep(1)
                if current["ready"] and args.exercise_requests:
                    current["requests"] = exercise_index(port)
                    current["after_requests_memory"] = memory_sample(container)
                current["storage_bytes"] = {
                    file.name: file.stat().st_size
                    for file in data.iterdir()
                    if file.is_file()
                }
                if not current["ready"]:
                    break
                if run < args.restarts:
                    docker("stop", "--time", "300", container, timeout=315)
                    stopped = json.loads(docker("inspect", container).stdout)[0][
                        "State"
                    ]
                    current["shutdown_exit_code"] = stopped["ExitCode"]
                    # JVM/native signal handlers may preserve the standard SIGTERM status.
                    if stopped["ExitCode"] not in (0, 143) or stopped["OOMKilled"]:
                        raise RuntimeError(
                            f"benchmark shutdown failed: {stopped['ExitCode']}"
                        )
                    if args.object_index_layout == "sqlite":
                        database = data / "s3-objects.sqlite"
                        connection = sqlite3.connect(
                            database.as_uri() + "?mode=ro", uri=True
                        )
                        try:
                            metadata = dict(connection.execute("SELECT k,v FROM meta"))
                        finally:
                            connection.close()
                        if (
                            metadata["revision"] != metadata["exported"]
                            or "export.json" in metadata
                        ):
                            raise RuntimeError(
                                "benchmark shutdown left unexported SQLite metadata"
                            )
                        current["shutdown_export_verified"] = True
            logs = docker("logs", "--tail", "100", container, check=False)
            result["logs"] = logs.stdout + logs.stderr
        finally:
            # Only the container created above is stopped. No Compose project or existing data is used.
            try:
                docker("stop", "--time", "300", container, check=False, timeout=315)
            finally:
                docker("rm", "--force", container, check=False)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        required=True,
        help="An image already present in the local Docker daemon",
    )
    parser.add_argument(
        "--object-index-layout", choices=["json", "sqlite"], default="json"
    )
    parser.add_argument(
        "--exercise-requests",
        action="store_true",
        help="Run bounded listings and 32 PUTs after readiness",
    )
    parser.add_argument(
        "--restarts",
        type=int,
        default=0,
        help="Reopen the same synthetic index after clean shutdown",
    )
    parser.add_argument("--entries", type=int, default=100_000)
    parser.add_argument("--metadata-bytes", type=int, default=256)
    parser.add_argument("--memory", default="8g")
    parser.add_argument("--cpus", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument(
        "--entrypoint",
        type=Path,
        help="Test an entrypoint patch with an existing image",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if (
        args.entries < 1
        or args.metadata_bytes < 0
        or args.timeout <= 0
        or args.cpus <= 0
        or args.restarts < 0
    ):
        parser.error(
            "entries, timeout and cpus must be positive; metadata-bytes must be nonnegative"
        )
    if args.entrypoint and not args.entrypoint.is_file():
        parser.error("entrypoint must be an existing file")
    result = benchmark(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"logs", "samples", "state", "restarts"}
            },
            indent=2,
        )
    )
    return (
        0 if result["ready"] and all(run["ready"] for run in result["restarts"]) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
