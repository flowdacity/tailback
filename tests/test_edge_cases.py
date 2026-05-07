# -*- coding: utf-8 -*-
#  Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.


import unittest
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import patch

from tailback import Tailback
from tailback.config import TailbackConfig
from tailback.exceptions import BadArgumentException, TailbackException
from tailback.redis import create_async_redis_client, create_sync_redis_client
from tailback.responses import format_queue_ids
from tailback.utils import is_valid_identifier
from tests.config import build_test_config


class FakeCluster:
    def __init__(
        self,
        startup_nodes=None,
        decode_responses=False,
        password=None,
        socket_timeout=None,
    ):
        self.startup_nodes = startup_nodes or []
        self.decode_responses = decode_responses
        self.password = password
        self.socket_timeout = socket_timeout

    def register_script(self, _):
        async def _runner(*args, **kwargs):
            return []

        return _runner

    async def ping(self):
        return True


class FakeRedisForClose:
    def __init__(self):
        self.closed = False
        self.waited = False
        self.disconnected = False
        self.connection_pool = self

    async def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True

    async def disconnect(self):
        self.disconnected = True


class FakeRedisForDeepStatus:
    def __init__(self):
        self.key_set = None

    async def set(self, key, value):
        self.key_set = (key, value)
        return True


class FakeRedisConnectionFailure:
    def __init__(self, *args, **kwargs):
        self.init_args = args
        self.init_kwargs = kwargs

    async def ping(self):
        raise ConnectionError("boom")

    def register_script(self, _):
        async def _runner(*args, **kwargs):
            return []

        return _runner


class FakeLuaDequeue:
    def __init__(self):
        self.called = False

    async def __call__(self, keys=None, args=None):
        self.called = True
        return [b"q1", b"j1", None, b"0"]


class FakePipe:
    def __init__(self):
        self.hdel_calls = []
        self.deleted = []
        self.executed = False

    def hdel(self, *args):
        self.hdel_calls.append(args)

    def delete(self, *args):
        self.deleted.append(args)

    async def execute(self):
        self.executed = True


class FakeRedisForClear:
    def __init__(self):
        self.pipe = FakePipe()
        self.deleted_keys = []

    async def zrem(self, _primary_set, _queue_id):
        return 1

    async def lrange(self, _key, _start, _end):
        return [None, b"job-bytes", "job-str"]

    def pipeline(self):
        return self.pipe

    async def delete(self, key):
        self.deleted_keys.append(key)


class RecordingRedisClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncSetUp(self):
        self.config = build_test_config()
        self.queue_instance = None

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncTearDown(self):
        """Clean up Redis state and close connections after each test."""
        # If a test initialized Tailback with real Redis, clean up
        if self.queue_instance is not None:
            with suppress(Exception):
                if self.queue_instance._r is not None:
                    await self.queue_instance._r.flushdb()
                await self.queue_instance.close()
            self.queue_instance = None

    def test_invalid_config_type_raises(self):
        with self.assertRaisesRegex(TailbackException, "Config must be a mapping"):
            Tailback("does-not-exist.conf")

    def test_missing_required_config_section_raises(self):
        config = build_test_config()
        del config["queue"]
        with self.assertRaisesRegex(
            TailbackException, "Config missing required sections: redis, queue"
        ):
            Tailback(config)

    def test_tailback_config_section_is_not_supported(self):
        config = build_test_config()
        config["tailback"] = config.pop("queue")
        with self.assertRaisesRegex(
            TailbackException, "Config missing required sections: redis, queue"
        ):
            Tailback(config)

    async def test_initialize_fails_fast_on_bad_redis(self):
        with patch("tailback.redis.AsyncRedis", FakeRedisConnectionFailure):
            queue = Tailback(self.config)
            with self.assertRaisesRegex(TailbackException, "Failed to connect to Redis"):
                await queue.initialize()

    async def test_cluster_initialization(self):
        """Covers clustered Redis path (queue.py lines 69-75, 104-106)."""
        config = build_test_config(
            queue={"key_prefix": "test_tailback_cluster"},
            redis={
                "clustered": True,
                "password": "cluster-password",
            }
        )
        with patch("tailback.redis.AsyncRedisCluster", FakeCluster):
            queue = Tailback(config)
            await queue.initialize()
            self.assertIsInstance(queue.redis_client(), FakeCluster)
            self.assertEqual(queue.redis_client().password, "cluster-password")
            startup_node = queue.redis_client().startup_nodes[0]
            self.assertEqual(startup_node.host, "127.0.0.1")
            self.assertEqual(startup_node.port, 6379)
            await queue.close()

    def test_clustered_config_must_be_boolean(self):
        config = build_test_config(redis={"clustered": "true"})
        with self.assertRaisesRegex(
            TailbackException, "Invalid config: redis.clustered must be a boolean"
        ):
            Tailback(config)

    def test_unix_socket_clustered_config_must_be_boolean(self):
        config = build_test_config(
            redis={
                "conn_type": "unix_sock",
                "clustered": "true",
            }
        )
        with self.assertRaisesRegex(
            TailbackException, "Invalid config: redis.clustered must be a boolean"
        ):
            Tailback(config)

    def test_missing_required_config_key_raises_with_path(self):
        config = build_test_config()
        del config["queue"]["key_prefix"]
        with self.assertRaisesRegex(TailbackException, "Missing config: queue.key_prefix"):
            Tailback(config)

    def test_invalid_config_value_raises_with_path(self):
        config = build_test_config(queue={"job_expire_interval": "5000"})
        with self.assertRaisesRegex(
            TailbackException,
            "Invalid config: queue.job_expire_interval must be a positive integer",
        ):
            Tailback(config)

    def test_invalid_redis_port_range_raises(self):
        for port in (0, -1, 65536):
            with self.subTest(port=port):
                config = build_test_config(redis={"port": port})
                with self.assertRaisesRegex(
                    TailbackException,
                    "Invalid config: redis.port must be an integer between 1 and 65535",
                ):
                    Tailback(config)

    async def test_dequeue_payload_none(self):
        """Covers dequeue branch where payload is None (queue.py line 212)."""
        queue = Tailback(self.config)
        self.queue_instance = queue
        await queue.initialize()
        fake_dequeue = FakeLuaDequeue()
        queue._scripts = SimpleNamespace(dequeue=fake_dequeue)
        result = await queue.dequeue()
        self.assertEqual(result["status"], "failure")
        self.assertTrue(fake_dequeue.called)

    async def test_clear_queue_delete_only(self):
        """Covers clear_queue else branch (queue.py lines 499, 502)."""
        queue = Tailback(self.config)
        self.queue_instance = queue
        await queue.initialize()
        await queue._r.flushdb()
        response = await queue.clear_queue(queue_type="noqueue", queue_id="missing")
        self.assertEqual(response["status"], "Failure")

    async def test_close_fallback_paths(self):
        """Covers close() fallback paths (queue.py lines 528-549)."""
        queue = Tailback(self.config)
        queue._r = FakeRedisForClose()
        await queue.close()
        self.assertIsNone(queue._r)

    async def test_deep_status_calls_set(self):
        """Covers deep_status (queue.py line 420)."""
        queue = Tailback(self.config)
        queue._r = FakeRedisForDeepStatus()
        await queue.deep_status()
        self.assertEqual(
            queue._r.key_set,
            (
                "fq:deep_status:{}".format(queue.config.queue.key_prefix),
                "sharq_deep_status",
            ),
        )

    def test_is_valid_identifier_non_string(self):
        """Covers utils.is_valid_identifier non-string check (utils.py line 22)."""
        self.assertFalse(is_valid_identifier(123))
        self.assertFalse(is_valid_identifier(None))
        self.assertFalse(is_valid_identifier(["a"]))

    def test_format_queue_ids_deduplicates_ready_and_active_queues(self):
        queue_ids = format_queue_ids(
            ready_queues=[b"johndoe", b"ready-only"],
            active_queues=[b"johndoe:job-1", "active-only:job-2"],
        )

        self.assertEqual(set(queue_ids), {"johndoe", "ready-only", "active-only"})
        self.assertEqual(len(queue_ids), 3)

    def test_redis_factories_pass_password_to_unix_socket_clients(self):
        config = TailbackConfig.from_mapping(
            build_test_config(
                redis={
                    "conn_type": "unix_sock",
                    "password": "socket-password",
                }
            )
        )

        with patch("tailback.redis.AsyncRedis", RecordingRedisClient):
            async_client = create_async_redis_client(config.redis)
        with patch("tailback.redis.SyncRedis", RecordingRedisClient):
            sync_client = create_sync_redis_client(config.redis)

        self.assertEqual(async_client.kwargs["password"], "socket-password")
        self.assertEqual(sync_client.kwargs["password"], "socket-password")

    def test_redis_factories_pass_password_to_cluster_clients(self):
        config = TailbackConfig.from_mapping(
            build_test_config(
                redis={
                    "clustered": True,
                    "password": "cluster-password",
                }
            )
        )

        with patch("tailback.redis.AsyncRedisCluster", RecordingRedisClient):
            async_client = create_async_redis_client(config.redis)
        with patch("tailback.redis.SyncRedisCluster", RecordingRedisClient):
            sync_client = create_sync_redis_client(config.redis)

        self.assertEqual(async_client.kwargs["password"], "cluster-password")
        self.assertEqual(sync_client.kwargs["password"], "cluster-password")

    async def test_clear_queue_purge_all_with_mixed_job_ids(self):
        """Covers purge_all loop branches (queue.py lines 463-468, 474-479)."""
        queue = Tailback(self.config)
        queue._r = FakeRedisForClear()
        response = await queue.clear_queue("qt", "qid", purge_all=True)
        self.assertEqual(response["status"], "Success")
        self.assertTrue(queue._r.pipe.executed)

    async def test_get_queue_length_invalid_params(self):
        """Covers validation branches (queue.py lines 499, 502)."""
        queue = Tailback(self.config)
        with self.assertRaises(BadArgumentException):
            await queue.get_queue_length("bad type", "qid")
        with self.assertRaises(BadArgumentException):
            await queue.get_queue_length("qtype", "bad id")

    async def test_deep_status_real_redis(self):
        """Covers deep_status with real redis (queue.py line 420)."""
        queue = Tailback(self.config)
        self.queue_instance = queue
        await queue.initialize()
        result = await queue.deep_status()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
