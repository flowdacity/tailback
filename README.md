[![Run tests and upload coverage](https://github.com/flowdacity/queue-engine/actions/workflows/tests.yml/badge.svg)](https://github.com/flowdacity/queue-engine/actions/workflows/tests.yml)
[![codecov](https://codecov.io/github/flowdacity/queue-engine/graph/badge.svg?token=70BDRZY956)](https://codecov.io/github/flowdacity/queue-engine)

Flowdacity Queue
================

Flowdacity Queue (FQ) is a rate-limited job queue built on Redis. It stores jobs per queue type and queue id, enforces per-queue dequeue intervals, automatically requeues expired jobs, and exposes metrics to understand throughput and queue depth.

## Features

- Per-queue rate limiting using millisecond intervals.
- Async and sync interfaces backed by Redis clients from redis-py.
- Lua scripts for predictable queue behavior.
- Automatic retries with configurable limits (including infinite retries).
- Metrics for enqueue/dequeue counts and queue lengths.
- Works with TCP or Unix socket Redis deployments and supports Redis Cluster.

## Requirements

- Python 3.12+
- Redis 7+ (run your own instance or start the bundled dev container)

## Installation

From PyPI:
```
pip install flowdacity-queue
```

From source (editable):
```
pip install -e .
```

## Configuration

FQ accepts a simple config mapping. Intervals are in milliseconds.
```python
config = {
    "fq": {
        "job_expire_interval": 5000,
        "job_requeue_interval": 5000,
        "default_job_requeue_limit": -1,  # -1 retries forever, 0 means no retries
    },
    "redis": {
        "db": 0,
        "key_prefix": "queue_server",
        "conn_type": "tcp_sock",  # or "unix_sock"
        "host": "127.0.0.1",
        "port": 6379,
        "password": "",
        "clustered": False,
        "unix_socket_path": "/tmp/redis.sock",
    },
}
```

> If you connect via Unix sockets, uncomment the `unixsocket` lines in your `redis.conf`:
> ```
> unixsocket /var/run/redis/redis.sock
> unixsocketperm 755
> ```

## Async Usage

Import `FQ` from the top-level package:

```python
from fq import FQ
```

```python
import asyncio
import uuid
from fq import FQ


async def main():
    config = {
        "fq": {
            "job_expire_interval": 5000,
            "job_requeue_interval": 5000,
            "default_job_requeue_limit": -1,
        },
        "redis": {
            "db": 0,
            "key_prefix": "queue_server",
            "conn_type": "tcp_sock",
            "host": "127.0.0.1",
            "port": 6379,
            "password": "",
            "clustered": False,
            "unix_socket_path": "/tmp/redis.sock",
        },
    }

    fq = FQ(config)
    await fq.initialize()  # connect to Redis and register Lua scripts

    job_id = str(uuid.uuid4())
    await fq.enqueue(
        payload={"message": "hello, world"},
        interval=1000,            # ms between successful dequeues
        job_id=job_id,
        queue_id="user001",
        queue_type="sms",
    )

    job = await fq.dequeue(queue_type="sms")
    if job["status"] == "success":
        # ...process job["payload"]...
        await fq.finish(
            queue_type="sms",
            queue_id=job["queue_id"],
            job_id=job["job_id"],
        )

    await fq.close()


asyncio.run(main())
```

## Sync Usage

Import `FQ` from `fq.sync`:

```python
import uuid
from fq.sync import FQ


config = {
    "fq": {
        "job_expire_interval": 5000,
        "job_requeue_interval": 5000,
        "default_job_requeue_limit": -1,
    },
    "redis": {
        "db": 0,
        "key_prefix": "queue_server",
        "conn_type": "tcp_sock",
        "host": "127.0.0.1",
        "port": 6379,
        "password": "",
        "clustered": False,
        "unix_socket_path": "/tmp/redis.sock",
    },
}

fq = FQ(config)
fq.initialize()

job_id = str(uuid.uuid4())
fq.enqueue(
    payload={"message": "hello, world"},
    interval=1000,
    job_id=job_id,
    queue_id="user001",
    queue_type="sms",
)

job = fq.dequeue(queue_type="sms")
if job["status"] == "success":
    fq.finish(
        queue_type="sms",
        queue_id=job["queue_id"],
        job_id=job["job_id"],
    )

fq.close()
```

## Common Operations

- `await fq.requeue()` — move expired jobs back onto their queues.
- `await fq.interval(interval=5000, queue_id="user001", queue_type="sms")` — change a queue’s rate limit on the fly.
- `await fq.metrics()` — global metrics; pass `queue_type` and/or `queue_id` for scoped stats and queue length.
- `await fq.clear_queue(queue_type="sms", queue_id="user001", purge_all=True)` — drop queued jobs and their payload/interval metadata.

The same operations are available from `fq.sync.FQ` without `await`.

## Development

- Start Redis for local development: `make redis-up` (binds to `localhost:6379`).
- Run the suite: `make test` (automatically starts and tears down Redis).
- Build a wheel: `make build`
- Install/uninstall from the build: `make install` / `make uninstall`
- Stop the dev Redis container: `make redis-down`

## License

MIT — see `LICENSE.txt`.
