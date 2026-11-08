#!/usr/bin/env python3
"""Measure cold S3 metadata startup using synthetic data and an isolated container."""

import argparse
import http.client
import json
import subprocess
import tempfile
import time
import uuid
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


def benchmark(args):
    # A missing image is an error, rather than an unexpected pull during a timed run.
    image = json.loads(docker("image", "inspect", args.image).stdout)[0]
    result = {
        "image": args.image,
        "image_id": image["Id"],
        "metadata_entries": args.entries,
        "metadata_padding_bytes": args.metadata_bytes,
        "memory_limit": args.memory,
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
        if args.entrypoint:
            command += [
                "--mount",
                f"type=bind,source={args.entrypoint.resolve()},target=/benchmark-entrypoint.sh,readonly",
                "--entrypoint",
                "/benchmark-entrypoint.sh",
            ]
        container = docker(*command, args.image).stdout.strip()
        try:
            started = time.monotonic()
            docker("start", container)
            port = int(
                json.loads(docker("inspect", container).stdout)[0]["NetworkSettings"][
                    "Ports"
                ]["4566/tcp"][0]["HostPort"]
            )
            while time.monotonic() - started < args.timeout:
                state = json.loads(docker("inspect", container).stdout)[0]["State"]
                result["state"] = state
                if not state["Running"]:
                    break
                sample = memory_sample(container)
                sample["elapsed_seconds"] = round(time.monotonic() - started, 3)
                result["samples"].append(sample)
                if head_bucket(port) == 200:
                    result["ready"] = True
                    result["s3_ready_seconds"] = round(time.monotonic() - started, 3)
                    result["ready_memory"] = memory_sample(container)
                    break
                time.sleep(1)
            logs = docker("logs", "--tail", "100", container, check=False)
            result["logs"] = logs.stdout + logs.stderr
        finally:
            # Only the container created above is stopped. No Compose project or existing data is used.
            try:
                docker("stop", "--time", "60", container, check=False, timeout=75)
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
                if key not in {"logs", "samples", "state"}
            },
            indent=2,
        )
    )
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
