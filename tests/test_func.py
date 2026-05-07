# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import uuid
import math
import asyncio
import unittest
import msgpack
from os.path import join
from tempfile import gettempdir
from unittest.mock import AsyncMock, MagicMock
from tailback import Tailback
from tailback.exceptions import TailbackException
from tailback.utils import generate_epoch, deserialize_payload
from tests.config import build_test_config


NONEXISTENT_UNIX_SOCKET_PATH = join(gettempdir(), "redis_nonexistent.sock")


class TailbackTestCase(unittest.IsolatedAsyncioTestCase):
    """
    `TailbackTestCase` contains the functional test cases
    that validate the correctness of all the APIs exposed
    by Tailback.
    """

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncSetUp(self):
        self.queue = Tailback(build_test_config())
        # flush all the keys in the test db before starting test
        await self.queue.initialize()
        await self.queue._r.flushdb()
        # test specific values
        self._test_queue_id = "johndoe"
        self._test_queue_type = "sms"
        self._test_payload_1 = {"to": "1000000000", "message": "Hello, world"}
        self._test_payload_2 = {"to": "1000000001", "message": "Hello, Tailback"}
        self._test_requeue_limit_5 = 5
        self._test_requeue_limit_neg_1 = -1
        self._test_requeue_limit_0 = 0
        self._test2_queue_id = "thetourist"
        self._test2_queue_type = "package"

    def _get_job_id(self):
        """Generates a uuid4 and returns the string
        representation of it.
        """
        return str(uuid.uuid4())

    async def test_enqueue_response_status(self):
        job_id = self._get_job_id()
        response = await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "queued")

    async def test_enqueue_job_queue_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertTrue(await self.queue._r.exists(queue_name))

    async def test_enqueue_job_existence_in_job_queue(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        latest_job_id = await self.queue._r.lrange(queue_name, -1, -1)
        latest_job_id = [jid.decode("utf-8") for jid in latest_job_id]
        self.assertEqual(latest_job_id, [job_id])

    async def test_enqueue_job_queue_length(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        queue_length = await self.queue._r.llen(queue_name)
        self.assertEqual(queue_length, 1)

    async def test_enqueue_payload_dump(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        payload_map_name = "%s:payload" % (self.queue._key_prefix)
        self.assertTrue(await self.queue._r.exists(payload_map_name))

    async def test_enqueue_payload_encode_decode(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        payload_map_name = "%s:payload" % (self.queue._key_prefix)
        payload_map_key = "%s:%s:%s" % (
            self._test_queue_type,
            self._test_queue_id,
            job_id,
        )
        raw_payload = await self.queue._r.hget(payload_map_name, payload_map_key)
        payload = deserialize_payload(raw_payload)
        self.assertEqual(payload, self._test_payload_1)

    async def test_enqueue_interval_map_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        self.assertTrue(await self.queue._r.exists(interval_map_name))

    async def test_enqueue_interval_value(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        interval_map_key = "%s:%s" % (self._test_queue_type, self._test_queue_id)
        interval = await self.queue._r.hget(interval_map_name, interval_map_key)
        interval = interval.decode("utf-8")
        self.assertEqual(interval, "10000")

    async def test_enqueue_requeue_limit_map_existence(self):
        # without requeue_limit
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        requeue_limit_map_name = "%s:%s:%s:requeues_remaining" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertTrue(await self.queue._r.exists(requeue_limit_map_name))

        # with requeue_limit
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5,
        )
        self.assertTrue(await self.queue._r.exists(requeue_limit_map_name))

    async def test_enqueue_requeue_limit_value(self):
        # without requeue_limit (from config)
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        requeue_limit_map_name = "%s:%s:%s:requeues_remaining" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        requeues_remaining = await self.queue._r.hget(requeue_limit_map_name, job_id)
        requeues_remaining = requeues_remaining.decode("utf-8")
        self.assertEqual(requeues_remaining, "-1")

        # with requeue_limit passed explicitly
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5,
        )

        requeues_remaining = await self.queue._r.hget(requeue_limit_map_name, job_id)
        requeues_remaining = requeues_remaining.decode("utf-8")
        self.assertEqual(requeues_remaining, "5")

    async def test_enqueue_ready_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        sorted_set_name = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        self.assertTrue(await self.queue._r.exists(sorted_set_name))

    async def test_enqueue_ready_set_contents(self):
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        end_time = str(generate_epoch())

        sorted_set_name = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        queue_id_list = await self.queue._r.zrangebyscore(
            sorted_set_name, start_time, end_time
        )
        queue_id_list = [qid.decode("utf-8") for qid in queue_id_list]
        self.assertEqual(len(queue_id_list), 1)
        self.assertEqual(queue_id_list[0], self._test_queue_id)

    async def test_enqueue_queue_type_ready_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_ready_set = await self.queue._r.smembers(
            "%s:ready:queue_type" % self.queue._key_prefix
        )
        queue_type_ready_set = {v.decode("utf-8") for v in queue_type_ready_set}
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    async def test_enqueue_queue_type_active_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_active_set = await self.queue._r.smembers(
            "%s:active:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_active_set), 0)

    async def test_enqueue_metrics_global_enqueue_counter(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = await self.queue._r.get(
            "%s:enqueue_counter:%s" % (self.queue._key_prefix, timestamp_minute)
        )
        counter_value = counter_value.decode("utf-8")
        self.assertEqual(counter_value, "1")

    async def test_enqueue_metrics_per_queue_enqueue_counter(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = await self.queue._r.get(
            "%s:%s:%s:enqueue_counter:%s"
            % (
                self.queue._key_prefix,
                self._test_queue_type,
                self._test_queue_id,
                timestamp_minute,
            )
        )
        counter_value = counter_value.decode("utf-8")
        self.assertEqual(counter_value, "1")

    async def test_enqueue_second_job_status(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        response = await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "queued")

    async def test_enqueue_second_job_queue_existence(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertTrue(await self.queue._r.exists(queue_name))

    async def test_enqueue_second_job_existence_in_job_queue(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        latest_job_id = await self.queue._r.lrange(queue_name, -1, -1)
        latest_job_id = [jid.decode("utf-8") for jid in latest_job_id]
        self.assertEqual(latest_job_id, [job_id])

    async def test_enqueue_second_job_queue_length(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        queue_length = await self.queue._r.llen(queue_name)
        self.assertEqual(queue_length, 2)

    async def test_enqueue_second_job_payload_dump(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        payload_map_name = "%s:payload" % (self.queue._key_prefix)
        self.assertTrue(await self.queue._r.exists(payload_map_name))

    async def test_enqueue_second_job_payload_encode_decode(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        payload_map_name = "%s:payload" % (self.queue._key_prefix)
        payload_map_key = "%s:%s:%s" % (
            self._test_queue_type,
            self._test_queue_id,
            job_id,
        )
        raw_payload = await self.queue._r.hget(payload_map_name, payload_map_key)
        payload = deserialize_payload(raw_payload)
        self.assertEqual(payload, self._test_payload_2)

    async def test_enqueue_second_job_interval_map_existence(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        self.assertTrue(await self.queue._r.exists(interval_map_name))

    async def test_enqueue_second_job_interval_value(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        interval_map_key = "%s:%s" % (self._test_queue_type, self._test_queue_id)
        interval = await self.queue._r.hget(interval_map_name, interval_map_key)
        interval = interval.decode("utf-8")
        self.assertEqual(interval, "20000")

    async def test_enqueue_second_job_ready_set(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        sorted_set_name = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        self.assertTrue(await self.queue._r.exists(sorted_set_name))

    async def test_enqueue_second_job_ready_set_contents(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await asyncio.sleep(0.5)
        # job 2
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        sorted_set_name = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        end_time = str(generate_epoch())
        queue_id_list = await self.queue._r.zrangebyscore(
            sorted_set_name, start_time, end_time
        )
        self.assertEqual(len(queue_id_list), 0)

    async def test_enqueue_second_job_queue_type_ready_set(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_ready_set = await self.queue._r.smembers(
            "%s:ready:queue_type" % self.queue._key_prefix
        )
        queue_type_ready_set = {v.decode("utf-8") for v in queue_type_ready_set}
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    async def test_enqueue_second_job_queue_type_active_set(self):
        # job 1
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # job 2
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_active_set = await self.queue._r.smembers(
            "%s:active:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_active_set), 0)

    async def test_dequeue_response_status_failure(self):
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "failure")

    async def test_dequeue_response_status_success_without_requeue_limit(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["queue_id"], self._test_queue_id)
        self.assertEqual(response["job_id"], job_id)
        self.assertEqual(response["payload"], self._test_payload_1)
        self.assertEqual(response["requeues_remaining"], -1)

    async def test_dequeue_response_status_success_with_requeue_limit(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["queue_id"], self._test_queue_id)
        self.assertEqual(response["job_id"], job_id)
        self.assertEqual(response["payload"], self._test_payload_1)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_5)

    async def test_dequeue_job_queue_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        queue_name = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertFalse(await self.queue._r.exists(queue_name))

    async def test_dequeue_time_keeper_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        time_keeper_key_name = "%s:%s:%s:time" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertTrue(await self.queue._r.exists(time_keeper_key_name))

    async def test_dequeue_ready_sorted_set_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        sorted_set_name = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        self.assertFalse(await self.queue._r.exists(sorted_set_name))

    async def test_dequeue_active_sorted_set(self):
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        active_sorted_set_name = "%s:%s:active" % (
            self.queue._key_prefix,
            self._test_queue_type,
        )
        end_time = str(generate_epoch())
        job_expire_timestamp = str(int(end_time) + self.queue._job_expire_interval)
        job_id_list = await self.queue._r.zrangebyscore(
            active_sorted_set_name, start_time, job_expire_timestamp
        )
        self.assertEqual(len(job_id_list), 1)

    async def test_dequeue_time_keeper_expiry(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=1000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        time_keeper_key_name = "%s:%s:%s:time" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertFalse(await self.queue._r.exists(time_keeper_key_name))

    async def test_dequeue_ready_queue_type_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        queue_type_ready_set = await self.queue._r.smembers(
            "%s:ready:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_ready_set), 0)

    async def test_dequeue_active_queue_type_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        queue_type_active_set = await self.queue._r.smembers(
            "%s:active:queue_type" % self.queue._key_prefix
        )
        queue_type_active_set = {v.decode("utf-8") for v in queue_type_active_set}
        self.assertEqual(len(queue_type_active_set), 1)
        self.assertEqual(queue_type_active_set.pop(), self._test_queue_type)

    async def test_dequeue_metrics_global_dequeue_counter(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = await self.queue._r.get(
            "%s:dequeue_counter:%s" % (self.queue._key_prefix, timestamp_minute)
        )
        counter_value = counter_value.decode("utf-8")
        self.assertEqual(counter_value, "1")

    async def test_dequeue_metrics_per_queue_dequeue_counter(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = await self.queue._r.get(
            "%s:%s:%s:dequeue_counter:%s"
            % (
                self.queue._key_prefix,
                self._test_queue_type,
                self._test_queue_id,
                timestamp_minute,
            )
        )
        counter_value = counter_value.decode("utf-8")
        self.assertEqual(counter_value, "1")

    async def test_finish_on_empty_queue(self):
        job_id = self._get_job_id()
        response = await self.queue.finish(
            job_id=job_id, queue_id="doesnotexist", queue_type=self._test_queue_type
        )
        self.assertEqual(response["status"], "failure")

    async def test_finish_response_status(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "success")

    async def test_finish_ready_sorted_set_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        self.assertFalse(
            await self.queue._r.exists(
                "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
            )
        )

    async def test_finish_active_sorted_set_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        self.assertFalse(
            await self.queue._r.exists(
                "%s:%s:active" % (self.queue._key_prefix, self._test_queue_type)
            )
        )

    async def test_finish_payload_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )
        self.assertFalse(
            await self.queue._r.exists("%s:payload" % self.queue._key_prefix)
        )

    async def test_finish_interval_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )
        self.assertFalse(
            await self.queue._r.exists("%s:interval" % self.queue._key_prefix)
        )

    async def test_finish_requeue_limit_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_0,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        self.assertFalse(
            await self.queue._r.exists(
                "%s:%s:%s:requeues_remaining"
                % (self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
            )
        )

    async def test_finish_job_queue_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        self.assertFalse(
            await self.queue._r.exists(
                "%s:%s:%s"
                % (self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
            )
        )

    async def test_finish_time_keeper_expire(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        time_keeper_key_name = "%s:%s:%s:time" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        self.assertFalse(await self.queue._r.exists(time_keeper_key_name))

    async def test_finish_queue_type_ready_set_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        queue_type_ready_set = await self.queue._r.smembers(
            "%s:ready:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_ready_set), 0)

    async def test_finish_queue_type_active_set_existence(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        await self.queue.finish(
            job_id=job_id,
            queue_id=response["queue_id"],
            queue_type=self._test_queue_type,
        )

        queue_type_active_set = await self.queue._r.smembers(
            "%s:active:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_active_set), 0)

    async def test_requeue_active_sorted_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)
        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)

        await self.queue.requeue()

        self.assertFalse(
            await self.queue._r.exists(
                "%s:%s:active" % (self.queue._key_prefix, self._test_queue_type)
            )
        )

    async def test_requeue_queue_type_ready_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)
        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)

        await self.queue.requeue()

        queue_type_ready_set = await self.queue._r.smembers(
            "%s:ready:queue_type" % self.queue._key_prefix
        )
        queue_type_ready_set = {v.decode("utf-8") for v in queue_type_ready_set}
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    async def test_requeue_queue_type_active_set(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)
        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)

        await self.queue.requeue()

        queue_type_active_set = await self.queue._r.smembers(
            "%s:active:queue_type" % self.queue._key_prefix
        )
        self.assertEqual(len(queue_type_active_set), 0)

    async def test_requeue_requeue_limit_5(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5,
        )

        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_5)

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        await self.queue.requeue()
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_5 - 1)

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        await self.queue.requeue()
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_5 - 2)

    async def test_requeue_requeue_limit_0(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_0,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_0)

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        await self.queue.requeue()
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "failure")

    async def test_requeue_requeue_limit_neg_1(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_neg_1,
        )
        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_neg_1)

        for _ in range(4):
            await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
            await self.queue.requeue()
            response = await self.queue.dequeue(queue_type=self._test_queue_type)
            self.assertEqual(
                response["requeues_remaining"], self._test_requeue_limit_neg_1
            )

        await asyncio.sleep(self.queue._job_expire_interval / 1000.0)
        await self.queue.requeue()
        # final response value still infinite
        self.assertEqual(response["requeues_remaining"], self._test_requeue_limit_neg_1)

    async def test_interval_non_existent_queue(self):
        response = await self.queue.interval(
            interval=1000,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "failure")

        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        self.assertFalse(await self.queue._r.exists(interval_map_name))

    async def test_interval_existent_queue(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = "%s:interval" % (self.queue._key_prefix)
        self.assertTrue(await self.queue._r.exists(interval_map_name))

        interval_map_key = "%s:%s" % (self._test_queue_type, self._test_queue_id)
        interval = await self.queue._r.hget(interval_map_name, interval_map_key)
        interval = interval.decode("utf-8")
        self.assertEqual(interval, "10000")

        response = await self.queue.interval(
            interval=5000,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "success")

        self.assertTrue(await self.queue._r.exists(interval_map_name))
        interval = await self.queue._r.hget(interval_map_name, interval_map_key)
        interval = interval.decode("utf-8")
        self.assertEqual(interval, "5000")

    async def test_metrics_response_status(self):
        response = await self.queue.metrics()
        self.assertEqual(response["status"], "success")

        response = await self.queue.metrics(self._test_queue_type)
        self.assertEqual(response["status"], "success")

        response = await self.queue.metrics(self._test_queue_type, self._test_queue_id)
        self.assertEqual(response["status"], "success")

    async def test_metrics_response_queue_types(self):
        response = await self.queue.metrics()
        self.assertEqual(response["queue_types"], [])
        self.assertEqual(len(response["enqueue_counts"].values()), 10)
        self.assertEqual(sum(response["enqueue_counts"].values()), 0)
        self.assertEqual(len(response["dequeue_counts"].values()), 10)
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = await self.queue.metrics()
        self.assertEqual(response["queue_types"], [self._test_queue_type])
        self.assertEqual(sum(response["enqueue_counts"].values()), 1)
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)

        await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.metrics()
        self.assertEqual(sum(response["dequeue_counts"].values()), 1)

    async def test_metrics_response_queue_ids(self):
        response = await self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response["queue_ids"], [])

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = await self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response["queue_ids"], [self._test_queue_id])

        await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response["queue_ids"], [self._test_queue_id])

    async def test_metrics_response_enqueue_counts_list(self):
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(len(response["enqueue_counts"].values()), 10)
        self.assertEqual(sum(response["enqueue_counts"].values()), 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(sum(response["enqueue_counts"].values()), 1)

    async def test_metrics_response_dequeue_counts_list(self):
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(len(response["dequeue_counts"].values()), 10)
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)

        await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(sum(response["dequeue_counts"].values()), 1)

    async def test_metrics_response_queue_length(self):
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(response["queue_length"], 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(response["queue_length"], 1)

        await self.queue.dequeue(queue_type=self._test_queue_type)
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(response["queue_length"], 0)

    async def test_metrics_enqueue_sliding_window(self):
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(sum(response["enqueue_counts"].values()), 0)
        self.assertEqual(sum(global_response["enqueue_counts"].values()), 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 1)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 2)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 2)

        await asyncio.sleep(65)
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 2)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 2)

        old_1_timestamp_minute = timestamp_minute
        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 0)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 1)
        self.assertEqual(response["enqueue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["enqueue_counts"][old_1_timestamp_minute], 2)

        await asyncio.sleep(65)

        old_2_timestamp_minute = timestamp_minute
        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 0)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["enqueue_counts"][timestamp_minute], 1)
        self.assertEqual(response["enqueue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(response["enqueue_counts"][old_2_timestamp_minute], 1)
        self.assertEqual(global_response["enqueue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["enqueue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(global_response["enqueue_counts"][old_2_timestamp_minute], 1)

    async def test_metrics_dequeue_sliding_window(self):
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)
        self.assertEqual(sum(global_response["dequeue_counts"].values()), 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await self.queue.dequeue(queue_type=self._test_queue_type)

        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 1)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await asyncio.sleep(0.1)
        await self.queue.dequeue(queue_type=self._test_queue_type)

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 2)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 2)

        await asyncio.sleep(65)
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 2)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 2)

        old_1_timestamp_minute = timestamp_minute
        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 0)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await asyncio.sleep(0.1)
        await self.queue.dequeue(queue_type=self._test_queue_type)

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 1)
        self.assertEqual(response["dequeue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["dequeue_counts"][old_1_timestamp_minute], 2)

        await asyncio.sleep(65)

        old_2_timestamp_minute = timestamp_minute
        timestamp = int(generate_epoch())
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 0)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 0)

        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        await asyncio.sleep(0.1)
        await self.queue.dequeue(queue_type=self._test_queue_type)

        response = await self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        global_response = await self.queue.metrics()
        self.assertEqual(response["dequeue_counts"][timestamp_minute], 1)
        self.assertEqual(response["dequeue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(response["dequeue_counts"][old_2_timestamp_minute], 1)
        self.assertEqual(global_response["dequeue_counts"][timestamp_minute], 1)
        self.assertEqual(global_response["dequeue_counts"][old_1_timestamp_minute], 2)
        self.assertEqual(global_response["dequeue_counts"][old_2_timestamp_minute], 1)

    async def test_tailback_rate_limiting(self):
        job_id_1 = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=2000,
            job_id=job_id_1,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        job_id_2 = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_2,
            interval=2000,
            job_id=job_id_2,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["queue_id"], self._test_queue_id)
        self.assertEqual(response["job_id"], job_id_1)
        self.assertEqual(response["payload"], self._test_payload_1)

        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "failure")

        await asyncio.sleep(2)

        response = await self.queue.dequeue(queue_type=self._test_queue_type)
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["queue_id"], self._test_queue_id)
        self.assertEqual(response["job_id"], job_id_2)
        self.assertEqual(response["payload"], self._test_payload_2)

    async def test_clear_queue_without_purge(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        queue_clear_response = await self.queue.clear_queue(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id
        )
        self.assertEqual(queue_clear_response["status"], "Success")
        self.assertEqual(
            queue_clear_response["message"], "Successfully removed all queued calls"
        )

        job_queue_list = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        primary_set = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        primary_sorted_key = await self.queue._r.zrange(primary_set, 0, -1)
        primary_sorted_key = [qid.decode("utf-8") for qid in primary_sorted_key]
        self.assertNotIn(self._test_queue_id, primary_sorted_key)
        self.assertFalse(await self.queue._r.exists(job_queue_list))

    async def test_clear_queue_with_purge(self):
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        queue_clear_response = await self.queue.clear_queue(
            queue_type=self._test_queue_type,
            queue_id=self._test_queue_id,
            purge_all=True,
        )
        self.assertEqual(queue_clear_response["status"], "Success")
        self.assertEqual(
            queue_clear_response["message"],
            "Successfully removed all queued calls and purged related resources",
        )

        job_queue_list = "%s:%s:%s" % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        primary_set = "%s:%s" % (self.queue._key_prefix, self._test_queue_type)
        payload_hashset = "%s:payload" % (self.queue._key_prefix)
        job_payload_key = "%s:%s:%s" % (
            self._test_queue_type,
            self._test_queue_id,
            job_id,
        )
        interval_set = "%s:interval" % (self.queue._key_prefix)
        job_interval_key = "%s:%s" % (self._test_queue_type, self._test_queue_id)

        primary_sorted_key = await self.queue._r.zrange(primary_set, 0, -1)
        primary_sorted_key = [qid.decode("utf-8") for qid in primary_sorted_key]
        self.assertNotIn(self._test_queue_id, primary_sorted_key)
        self.assertFalse(
            await self.queue._r.hexists(payload_hashset, job_payload_key)
        )
        self.assertFalse(
            await self.queue._r.hexists(interval_set, job_interval_key)
        )
        self.assertFalse(await self.queue._r.exists(job_queue_list))

    async def test_clear_queue_with_non_existing_queue_id(self):
        queue_clear_response = await self.queue.clear_queue(
            queue_type=self._test2_queue_type, queue_id=self._test2_queue_id
        )
        self.assertEqual(queue_clear_response["status"], "Failure")
        self.assertEqual(queue_clear_response["message"], "No queued calls found")

    async def test_clear_queue_with_non_existing_queue_id_with_purge(self):
        queue_clear_response = await self.queue.clear_queue(
            queue_type=self._test2_queue_type,
            queue_id=self._test2_queue_id,
            purge_all=True,
        )
        self.assertEqual(queue_clear_response["status"], "Failure")
        self.assertEqual(queue_clear_response["message"], "No queued calls found")

    async def test_deep_status(self):
        """Test deep_status method for Redis availability check."""
        result = await self.queue.deep_status()
        self.assertIsNotNone(result)

    async def test_initialize_public_method(self):
        """Test the public initialize() method."""
        queue = Tailback(build_test_config())
        
        # Public initialize() should work
        await queue.initialize()
        
        # Verify initialization succeeded
        self.assertIsNotNone(queue._r)
        self.assertIsNotNone(queue._scripts.enqueue)
        
        # Cleanup
        await queue.close()

    async def test_reload_lua_scripts(self):
        """Test reload_lua_scripts method."""
        # Just verify it doesn't crash and scripts work after reload
        self.queue.reload_lua_scripts()
        
        # Verify scripts are still functional
        job_id = self._get_job_id()
        response = await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=1000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response["status"], "queued")

    async def test_get_queue_length(self):
        """Test get_queue_length method."""
        # Initially empty
        length = await self.queue.get_queue_length(
            self._test_queue_type, self._test_queue_id
        )
        self.assertEqual(length, 0)
        
        # Add a job
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=1000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        
        # Check length
        length = await self.queue.get_queue_length(
            self._test_queue_type, self._test_queue_id
        )
        self.assertEqual(length, 1)

    async def test_redis_client_getter(self):
        """Test redis_client() method."""
        client = self.queue.redis_client()
        self.assertIsNotNone(client)
        # Verify it's the same client
        self.assertIs(client, self.queue._r)

    async def test_close_properly_closes_connection(self):
        """Test close() method properly closes Redis connection."""
        queue = Tailback(build_test_config())
        await queue.initialize()
        
        self.assertIsNotNone(queue._r)
        await queue.close()
        self.assertIsNone(queue._r)

    async def test_close_with_none_client(self):
        """Test close() when redis client is None."""
        queue = Tailback(build_test_config())
        # Don't initialize, so _r is None
        await queue.close()  # Should not crash
        self.assertIsNone(queue._r)

    async def test_initialize_unix_socket_connection(self):
        """Test initialization with Unix socket connection - tests line 59."""
        config = build_test_config(
            queue={"key_prefix": "test_tailback_unix"},
            redis={
                "conn_type": "unix_sock",
                "unix_socket_path": NONEXISTENT_UNIX_SOCKET_PATH,
            }
        )

        # Create a mock Redis class to capture initialization parameters
        mock_redis_instance = MagicMock()
        mock_redis_instance.ping = AsyncMock(return_value=True)
        mock_redis_instance.register_script = MagicMock(return_value=MagicMock())
        mock_redis_instance.aclose = AsyncMock()

        redis_init_kwargs = {}

        def mock_redis_constructor(**kwargs):
            redis_init_kwargs.update(kwargs)
            return mock_redis_instance

        # Patch Redis to intercept the initialization
        with unittest.mock.patch(
            "tailback.redis.AsyncRedis",
            side_effect=mock_redis_constructor,
        ):
            queue = Tailback(config)
            await queue.initialize()

            # Verify that Redis was initialized with unix_socket_path
            self.assertIn("unix_socket_path", redis_init_kwargs)
            self.assertEqual(
                redis_init_kwargs["unix_socket_path"], NONEXISTENT_UNIX_SOCKET_PATH
            )
            self.assertEqual(int(redis_init_kwargs["db"]), 0)

            await queue.close()

    async def test_initialize_unknown_connection_type(self):
        """Test constructor validation with invalid connection type."""
        config = build_test_config(redis={"conn_type": "invalid_type"})
        with self.assertRaisesRegex(
            TailbackException,
            "Invalid config: redis.conn_type must be 'tcp_sock' or 'unix_sock'",
        ):
            Tailback(config)

    async def test_clear_queue_with_purge_all_and_string_job_uuid(self):
        """Test clear_queue with purge_all=True handles string job UUIDs - tests lines 464, 468."""
        job_id = self._get_job_id()
        await self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        
        # Clear with purge_all
        result = await self.queue.clear_queue(
            queue_type=self._test_queue_type,
            queue_id=self._test_queue_id,
            purge_all=True
        )
        self.assertEqual(result["status"], "Success")
        self.assertIn("purged", result["message"])

    async def test_deserialize_payload_old_format(self):
        """Test deserialize_payload with old quote-wrapped format - tests utils.py line 63."""
        test_data = {"key": "value", "number": 42}
        # Simulate old format: msgpack wrapped in quotes
        packed = msgpack.packb(test_data, use_bin_type=True)
        old_format_payload = b'"' + packed + b'"'
        
        result = deserialize_payload(old_format_payload)
        self.assertEqual(result, test_data)

    async def test_deserialize_payload_new_format(self):
        """Test deserialize_payload handles new unwrapped format - tests utils.py line 63."""
        test_data = {"key": "value", "nested": {"inner": "data"}}
        packed = msgpack.packb(test_data, use_bin_type=True)
        
        result = deserialize_payload(packed)
        self.assertEqual(result, test_data)

    async def test_dequeue_empty_queue_returns_failure(self):
        """Test dequeue on empty queue returns failure status - tests queue.py line 212."""
        result = await self.queue.dequeue(queue_type="nonexistent_type")
        self.assertEqual(result["status"], "failure")
        # Verify no payload key in response
        self.assertNotIn("payload", result)

    async def test_deep_status_redis_availability(self):
        """Test deep_status method checks Redis availability - tests queue.py line 420."""
        result = await self.queue.deep_status()
        # Should succeed with Redis running
        self.assertIsNotNone(result)

    async def test_convert_to_str_with_mixed_types(self):
        """Test convert_to_str handles both bytes and strings."""
        from tailback.utils import convert_to_str
        
        # Mixed bytes and string set
        mixed_set = {b"key1", "key2", b"key3"}
        result = convert_to_str(mixed_set)
        
        # All should be strings
        self.assertTrue(all(isinstance(x, str) for x in result))
        self.assertIn("key1", result)
        self.assertIn("key2", result)
        self.assertIn("key3", result)

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncTearDown(self):
        await self.queue._r.flushdb()
        await self.queue.close()


def main():
    unittest.main()


if __name__ == "__main__":
    main()
