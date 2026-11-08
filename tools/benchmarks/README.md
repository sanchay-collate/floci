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
