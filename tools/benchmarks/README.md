# S3 metadata startup benchmark

Run fixture checks without Docker:

```sh
python3 -m unittest discover -s tools/benchmarks -p 'test_*.py'
```

The benchmark streams synthetic, versioned S3 metadata into a fresh temporary
folder and starts one disposable container. It does not use Compose, mount the
Docker socket, copy real object data, or alter an existing service. Only a random
port on host loopback is published. The image must already exist locally.

```sh
python3 tools/benchmarks/s3_metadata.py \
  --image floci/floci:2.4.0 --entries 2000000 \
  --output /tmp/floci-metadata-baseline.json
```

Use the same arguments with a fork image for comparison. To test only an
entrypoint change against an existing image, add `--entrypoint docker/entrypoint.sh`.
Two metadata entries represent one logical object's latest alias and version
record. The fixture contains metadata only, with no object bodies or old
multipart files. It does not model the cost of traversing millions of body files.

The result records the immutable image ID, fixture size, elapsed time until an
S3 bucket HEAD returns 200, process RSS and high-water RSS, and cgroup current and
peak memory. Cgroup memory includes charged file cache, so compare it separately
from process RSS. The benchmark uses up to the specified memory and CPU limits;
a two-million-entry snapshot is intentionally a large workload. A timeout or OOM
is recorded as a failed run. Startup time includes container launch and a polling
interval. This is a cold process start, not a claim that the host's filesystem
cache is cold. Object reads, writes, notification delivery and crash recovery
need separate compatibility tests before replacing a metadata backend.

For the disk-backed layout, use a locally built fork image and retain the synthetic
index for one clean restart:

```sh
python3 tools/benchmarks/s3_metadata.py \
  --image floci-fork:sqlite --object-index-layout sqlite \
  --entries 2000000 --memory 2g --timeout 900 --restarts 1 \
  --output /tmp/floci-metadata-sqlite.json
```

The first run measures migration. The `restarts` array records subsequent process
starts using the same SQLite database. The script stops only its own container between
runs. Each run records file sizes as well as memory, so the extra database and migration
backup space are visible. Keep the image, CPU limit, dataset, and cache conditions the
same when comparing JSON and SQLite; results from a different JVM/native runtime are
not a controlled storage-only comparison.

The boto3 smoke test creates a separate disposable SQLite container and checks SDK
pagination, versions, delete markers, tags, multipart data, presigned uploads, S3 to
SQS notifications, clean restart, SIGKILL recovery, and offline JSON cleanup:

```sh
uv run --no-project --with boto3==1.40.61 python tools/benchmarks/s3_sqlite_smoke.py \
  --image floci-fork:sqlite --output /tmp/floci-sqlite-sdk.json
```

It accepts an image only, never an existing endpoint. All service calls and restarts
are limited to its own container and temporary data directory.

## Recorded SQLite comparison

The [2026-11-08 result](results/sqlite-2026-11-08.json) used the same Linux ARM64
native image for both layouts, two CPUs, and a 1,565,000,001-byte synthetic snapshot:
two million metadata rows representing one million latest/history pairs. JSON had
an 8 GiB container limit; SQLite had 2 GiB and a 16 MiB page-cache budget.

| Operation | Time to S3 HEAD 200 | Process RSS at readiness | Process peak RSS |
|---|---:|---:|---:|
| JSON process start | 55.448 s | 4.10 GiB | 6.80 GiB |
| SQLite first import | 254.927 s | 642 MiB | 931 MiB |
| SQLite reopen after 32 PUTs | 13.334 s | 143 MiB | 143 MiB |

The SQLite database was approximately 3.54 GiB. Migration retained the original
snapshot and a metadata backup; export requires more temporary disk space. A clean
shutdown exported the index successfully before reopening. The large run exercised
1,000-entry object/version listings, delimiter grouping, and 32 PUTs per process.

These are metadata-only observations on a running development machine, with warm
filesystem caches possible. Cgroup peaks include reclaimable file cache. The first
cold delimiter request took 10.57 s and motivated the cursor-range correction; a
50,000-row regression now aborts queries exceeding 10,000 SQLite execution steps
when skipping ranges or resuming deep pages. The full-scale measurements precede
that correction and the additional Error-rollback safeguard, so they are retained
as a baseline rather than presented as a retiming of the final image. See the SDK
smoke test and the storage/S3 regression tests for correctness and recovery coverage.
