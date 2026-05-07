[![Run tests and upload coverage](https://github.com/flowdacity/tailback/actions/workflows/tests.yml/badge.svg)](https://github.com/flowdacity/tailback/actions/workflows/tests.yml)
[![codecov](https://codecov.io/github/flowdacity/tailback/graph/badge.svg?token=70BDRZY956)](https://codecov.io/github/flowdacity/tailback)

Tailback
================

Tailback is a flexible, open-source, rate-limited queuing system. Based on the [Leaky Bucket Algorithm](http://en.wikipedia.org/wiki/Leaky_bucket#The_Leaky_Bucket_Algorithm_as_a_Queue), Tailback lets you create queues dynamically and update their rate limits in real time.

## Features

- Dynamic queues with no setup step.
- Per-queue rate limits.
- Live rate limit updates.
- Automatic retries for unfinished jobs.
- Simple queue metrics.
- Async and sync Python support.

## Requirements

- Python 3.12+
- Redis 7+ (run your own instance or start the bundled dev container)

## Installation

From PyPI:
```
pip install tailback
```

From source (editable):
```
pip install -e .
```

## Configuration

Tailback accepts a simple config mapping. Intervals are in milliseconds.
```python
config = {
    "queue": {
        "key_prefix": "queue_server",
        "job_expire_interval": 5000,
        "job_requeue_interval": 5000,
        "default_job_requeue_limit": -1,  # -1 retries forever, 0 means no retries
    },
    "redis": {
        "db": 0,
        "conn_type": "tcp_sock",
        "host": "127.0.0.1",
        "port": 6379,
        "password": "",
        "clustered": False,
    },
}
```

For Unix socket connections, use `conn_type: "unix_sock"` and provide
`unix_socket_path`:
```python
"redis": {
    "db": 0,
    "conn_type": "unix_sock",
    "unix_socket_path": "/tmp/redis.sock",
    "password": "",
    "clustered": False,
}
```

> If you use Unix sockets, uncomment the `unixsocket` lines in your `redis.conf`:
> ```
> unixsocket /var/run/redis/redis.sock
> unixsocketperm 755
> ```

## Async Usage

Import `Tailback` from the top-level package:

```python
from tailback import Tailback
```

```python
import asyncio
import uuid
from tailback import Tailback


async def main():
    config = {
        "queue": {
            "key_prefix": "queue_server",
            "job_expire_interval": 5000,
            "job_requeue_interval": 5000,
            "default_job_requeue_limit": -1,
        },
        "redis": {
            "db": 0,
            "conn_type": "tcp_sock",
            "host": "127.0.0.1",
            "port": 6379,
            "password": "",
            "clustered": False,
        },
    }

    queue = Tailback(config)
    await queue.initialize()  # connect to Redis and register Lua scripts

    job_id = str(uuid.uuid4())
    await queue.enqueue(
        payload={"message": "hello, world"},
        interval=1000,            # ms between successful dequeues
        job_id=job_id,
        queue_id="user001",
        queue_type="sms",
    )

    job = await queue.dequeue(queue_type="sms")
    if job["status"] == "success":
        # ...process job["payload"]...
        await queue.finish(
            queue_type="sms",
            queue_id=job["queue_id"],
            job_id=job["job_id"],
        )

    await queue.close()


asyncio.run(main())
```

## Sync Usage

Import `Tailback` from `tailback.sync`:

```python
import uuid
from tailback.sync import Tailback


config = {
    "queue": {
        "key_prefix": "queue_server",
        "job_expire_interval": 5000,
        "job_requeue_interval": 5000,
        "default_job_requeue_limit": -1,
    },
    "redis": {
        "db": 0,
        "conn_type": "tcp_sock",
        "host": "127.0.0.1",
        "port": 6379,
        "password": "",
        "clustered": False,
    },
}

queue = Tailback(config)
queue.initialize()

job_id = str(uuid.uuid4())
queue.enqueue(
    payload={"message": "hello, world"},
    interval=1000,
    job_id=job_id,
    queue_id="user001",
    queue_type="sms",
)

job = queue.dequeue(queue_type="sms")
if job["status"] == "success":
    queue.finish(
        queue_type="sms",
        queue_id=job["queue_id"],
        job_id=job["job_id"],
    )

queue.close()
```

## Common Operations

- `await queue.requeue()` — move expired jobs back onto their queues.
- `await queue.interval(interval=5000, queue_id="user001", queue_type="sms")` — change a queue’s rate limit on the fly.
- `await queue.metrics()` — global metrics; pass `queue_type` and/or `queue_id` for scoped stats and queue length.
- `await queue.clear_queue(queue_type="sms", queue_id="user001", purge_all=True)` — drop queued jobs and their payload/interval metadata.

The same operations are available from `tailback.sync.Tailback` without `await`.

## Development

- Start Redis for local development: `make redis-up` (binds to `localhost:6379`).
- Run the suite: `make test` (automatically starts and tears down Redis).
- Build a wheel: `make build`
- Install/uninstall from the build: `make install` / `make uninstall`
- Stop the dev Redis container: `make redis-down`

## License

MIT — see `LICENSE.txt`.
