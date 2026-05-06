# -*- coding: utf-8 -*-
#  Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.


import unittest
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import patch

from fq import FQ
from fq.utils import is_valid_identifier
from fq.exceptions import BadArgumentException, FQException
from tests.config import build_test_config


class FakeCluster:
    def __init__(self, startup_nodes=None, decode_responses=False, socket_timeout=None):
        self.startup_nodes = startup_nodes or []
        self.decode_responses = decode_responses
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


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncSetUp(self):
        self.config = build_test_config()
        self.fq_instance = None

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncTearDown(self):
        """Clean up Redis state and close connections after each test."""
        # If a test initialized FQ with real Redis, clean up
        if self.fq_instance is not None:
            with suppress(Exception):
                if self.fq_instance._r is not None:
                    await self.fq_instance._r.flushdb()
                await self.fq_instance.close()
            self.fq_instance = None

    def test_invalid_config_type_raises(self):
        with self.assertRaisesRegex(FQException, "Config must be a mapping"):
            FQ("does-not-exist.conf")

    async def test_initialize_fails_fast_on_bad_redis(self):
        with patch("fq.redis.AsyncRedis", FakeRedisConnectionFailure):
            fq = FQ(self.config)
            with self.assertRaisesRegex(FQException, "Failed to connect to Redis"):
                await fq.initialize()

    async def test_cluster_initialization(self):
        """Covers clustered Redis path (queue.py lines 69-75, 104-106)."""
        config = build_test_config(
            redis={"key_prefix": "test_fq_cluster", "clustered": True}
        )
        with patch("fq.redis.AsyncRedisCluster", FakeCluster):
            fq = FQ(config)
            await fq.initialize()
            self.assertIsInstance(fq.redis_client(), FakeCluster)
            await fq.close()

    def test_clustered_config_must_be_boolean(self):
        config = build_test_config(redis={"clustered": "true"})
        with self.assertRaisesRegex(
            FQException, "Invalid config: redis.clustered must be a boolean"
        ):
            FQ(config)

    def test_missing_required_config_key_raises_with_path(self):
        config = build_test_config()
        del config["redis"]["key_prefix"]
        with self.assertRaisesRegex(FQException, "Missing config: redis.key_prefix"):
            FQ(config)

    def test_invalid_config_value_raises_with_path(self):
        config = build_test_config(fq={"job_expire_interval": "5000"})
        with self.assertRaisesRegex(
            FQException,
            "Invalid config: fq.job_expire_interval must be a positive integer",
        ):
            FQ(config)

    async def test_dequeue_payload_none(self):
        """Covers dequeue branch where payload is None (queue.py line 212)."""
        fq = FQ(self.config)
        self.fq_instance = fq
        await fq.initialize()
        fake_dequeue = FakeLuaDequeue()
        fq._scripts = SimpleNamespace(dequeue=fake_dequeue)
        result = await fq.dequeue()
        self.assertEqual(result["status"], "failure")
        self.assertTrue(fake_dequeue.called)

    async def test_clear_queue_delete_only(self):
        """Covers clear_queue else branch (queue.py lines 499, 502)."""
        fq = FQ(self.config)
        self.fq_instance = fq
        await fq.initialize()
        await fq._r.flushdb()
        response = await fq.clear_queue(queue_type="noqueue", queue_id="missing")
        self.assertEqual(response["status"], "Failure")

    async def test_close_fallback_paths(self):
        """Covers close() fallback paths (queue.py lines 528-549)."""
        fq = FQ(self.config)
        fq._r = FakeRedisForClose()
        await fq.close()
        self.assertIsNone(fq._r)

    async def test_deep_status_calls_set(self):
        """Covers deep_status (queue.py line 420)."""
        fq = FQ(self.config)
        fq._r = FakeRedisForDeepStatus()
        await fq.deep_status()
        self.assertEqual(
            fq._r.key_set,
            (
                "fq:deep_status:{}".format(fq.config.redis.key_prefix),
                "sharq_deep_status",
            ),
        )

    def test_is_valid_identifier_non_string(self):
        """Covers utils.is_valid_identifier non-string check (utils.py line 22)."""
        self.assertFalse(is_valid_identifier(123))
        self.assertFalse(is_valid_identifier(None))
        self.assertFalse(is_valid_identifier(["a"]))

    async def test_clear_queue_purge_all_with_mixed_job_ids(self):
        """Covers purge_all loop branches (queue.py lines 463-468, 474-479)."""
        fq = FQ(self.config)
        fq._r = FakeRedisForClear()
        response = await fq.clear_queue("qt", "qid", purge_all=True)
        self.assertEqual(response["status"], "Success")
        self.assertTrue(fq._r.pipe.executed)

    async def test_get_queue_length_invalid_params(self):
        """Covers validation branches (queue.py lines 499, 502)."""
        fq = FQ(self.config)
        with self.assertRaises(BadArgumentException):
            await fq.get_queue_length("bad type", "qid")
        with self.assertRaises(BadArgumentException):
            await fq.get_queue_length("qtype", "bad id")

    async def test_deep_status_real_redis(self):
        """Covers deep_status with real redis (queue.py line 420)."""
        fq = FQ(self.config)
        self.fq_instance = fq
        await fq.initialize()
        result = await fq.deep_status()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
