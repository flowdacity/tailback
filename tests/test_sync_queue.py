# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

import asyncio
import time
import unittest
import uuid

from fq import FQ as AsyncFQ
from fq.exceptions import BadArgumentException
from fq.sync import FQ
from tests.config import build_test_config


class SyncFQTest(unittest.TestCase):
    def setUp(self):
        self.queue = FQ(build_test_config(redis={"key_prefix": "test_fq_sync"}))
        self.queue.initialize()
        self.queue._r.flushdb()
        self.queue_type = "sms"
        self.queue_id = "johndoe"
        self.payload = {"to": "1000000000", "message": "Hello, sync FQ"}

    def tearDown(self):
        if self.queue is not None and self.queue._r is not None:
            self.queue._r.flushdb()
            self.queue.close()

    def _job_id(self):
        return str(uuid.uuid4())

    def test_import_namespace(self):
        from fq import FQ as ImportedAsyncFQ
        from fq.sync import FQ as ImportedSyncFQ

        self.assertIs(ImportedAsyncFQ, AsyncFQ)
        self.assertIs(ImportedSyncFQ, FQ)
        self.assertIsNot(ImportedAsyncFQ, ImportedSyncFQ)

    def test_initialize_close_and_reload_scripts(self):
        self.assertIs(self.queue.redis_client(), self.queue._r)
        self.assertIsNotNone(self.queue._scripts.enqueue)

        self.queue.reload_lua_scripts()
        self.assertIsNotNone(self.queue._scripts.enqueue)
        self.assertTrue(self.queue.deep_status())

        self.queue.close()
        self.assertIsNone(self.queue._r)
        self.queue.close()

    def test_enqueue_dequeue_finish(self):
        job_id = self._job_id()
        response = self.queue.enqueue(
            payload=self.payload,
            interval=1000,
            job_id=job_id,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
        )
        self.assertEqual(response, {"status": "queued"})
        self.assertEqual(self.queue.get_queue_length(self.queue_type, self.queue_id), 1)

        job = self.queue.dequeue(queue_type=self.queue_type)
        self.assertEqual(
            job,
            {
                "status": "success",
                "queue_id": self.queue_id,
                "job_id": job_id,
                "payload": self.payload,
                "requeues_remaining": -1,
            },
        )

        self.assertEqual(
            self.queue.finish(
                queue_type=self.queue_type,
                queue_id=job["queue_id"],
                job_id=job["job_id"],
            ),
            {"status": "success"},
        )
        self.assertEqual(
            self.queue.finish(
                queue_type=self.queue_type,
                queue_id=job["queue_id"],
                job_id=job["job_id"],
            ),
            {"status": "failure"},
        )

    def test_requeue_behavior(self):
        self.queue.close()
        self.queue = FQ(
            build_test_config(
                fq={"job_expire_interval": 20},
                redis={"key_prefix": "test_fq_sync_requeue"},
            )
        )
        self.queue.initialize()
        self.queue._r.flushdb()

        job_id = self._job_id()
        self.queue.enqueue(
            payload=self.payload,
            interval=1,
            job_id=job_id,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
            requeue_limit=1,
        )
        first_job = self.queue.dequeue(queue_type=self.queue_type)
        self.assertEqual(first_job["status"], "success")

        time.sleep(0.08)
        self.queue.requeue()
        self.assertEqual(self.queue.get_queue_length(self.queue_type, self.queue_id), 1)

        requeued_job = self.queue.dequeue(queue_type=self.queue_type)
        self.assertEqual(requeued_job["status"], "success")
        self.assertEqual(requeued_job["job_id"], job_id)
        self.assertEqual(requeued_job["requeues_remaining"], 0)

    def test_interval_update(self):
        job_id = self._job_id()
        self.queue.enqueue(
            payload=self.payload,
            interval=1000,
            job_id=job_id,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
        )

        response = self.queue.interval(
            interval=250,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
        )
        self.assertEqual(response, {"status": "success"})
        self.assertEqual(
            self.queue._r.hget(
                "%s:interval" % self.queue._key_prefix,
                "%s:%s" % (self.queue_type, self.queue_id),
            ),
            b"250",
        )

    def test_metrics(self):
        response = self.queue.metrics()
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["queue_types"], [])
        self.assertEqual(sum(response["enqueue_counts"].values()), 0)
        self.assertEqual(sum(response["dequeue_counts"].values()), 0)

        job_id = self._job_id()
        self.queue.enqueue(
            payload=self.payload,
            interval=1,
            job_id=job_id,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
        )

        queue_type_metrics = self.queue.metrics(queue_type=self.queue_type)
        self.assertEqual(queue_type_metrics["status"], "success")
        self.assertEqual(queue_type_metrics["queue_ids"], [self.queue_id])

        queue_metrics = self.queue.metrics(
            queue_type=self.queue_type,
            queue_id=self.queue_id,
        )
        self.assertEqual(queue_metrics["status"], "success")
        self.assertEqual(queue_metrics["queue_length"], 1)
        self.assertEqual(sum(queue_metrics["enqueue_counts"].values()), 1)

        self.queue.dequeue(queue_type=self.queue_type)
        global_metrics = self.queue.metrics()
        self.assertEqual(global_metrics["queue_types"], [self.queue_type])
        self.assertEqual(sum(global_metrics["dequeue_counts"].values()), 1)

    def test_clear_queue(self):
        job_id = self._job_id()
        self.queue.enqueue(
            payload=self.payload,
            interval=1000,
            job_id=job_id,
            queue_id=self.queue_id,
            queue_type=self.queue_type,
        )

        response = self.queue.clear_queue(
            queue_type=self.queue_type,
            queue_id=self.queue_id,
            purge_all=True,
        )
        self.assertEqual(
            response,
            {
                "status": "Success",
                "message": "Successfully removed all queued calls and purged related resources",
            },
        )
        self.assertEqual(self.queue.get_queue_length(self.queue_type, self.queue_id), 0)
        self.assertIsNone(
            self.queue._r.hget(
                "%s:interval" % self.queue._key_prefix,
                "%s:%s" % (self.queue_type, self.queue_id),
            )
        )

    def test_validation_errors_match_async_api(self):
        def collect_sync_errors():
            checks = [
                lambda: self.queue.enqueue(
                    payload=self.payload,
                    interval=0,
                    job_id=self._job_id(),
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: self.queue.dequeue(queue_type="bad type"),
                lambda: self.queue.finish(
                    job_id="bad id",
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: self.queue.interval(
                    interval=0,
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: self.queue.metrics(queue_id=self.queue_id),
                lambda: self.queue.clear_queue(
                    queue_type=self.queue_type,
                    queue_id="bad id",
                ),
                lambda: self.queue.get_queue_length("bad type", self.queue_id),
            ]
            errors = []
            for check in checks:
                with self.assertRaises(BadArgumentException) as ctx:
                    check()
                errors.append(str(ctx.exception))
            return errors

        async def collect_async_errors():
            queue = AsyncFQ(build_test_config(redis={"key_prefix": "test_fq_async"}))
            await queue.initialize()
            await queue._r.flushdb()
            checks = [
                lambda: queue.enqueue(
                    payload=self.payload,
                    interval=0,
                    job_id=self._job_id(),
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: queue.dequeue(queue_type="bad type"),
                lambda: queue.finish(
                    job_id="bad id",
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: queue.interval(
                    interval=0,
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                ),
                lambda: queue.metrics(queue_id=self.queue_id),
                lambda: queue.clear_queue(
                    queue_type=self.queue_type,
                    queue_id="bad id",
                ),
                lambda: queue.get_queue_length("bad type", self.queue_id),
            ]
            errors = []
            try:
                for check in checks:
                    with self.assertRaises(BadArgumentException) as ctx:
                        await check()
                    errors.append(str(ctx.exception))
                return errors
            finally:
                await queue._r.flushdb()
                await queue.close()

        self.assertEqual(collect_sync_errors(), asyncio.run(collect_async_errors()))

    def test_sync_async_interoperability(self):
        async def scenario():
            config = build_test_config(redis={"key_prefix": "test_fq_sync_interop"})
            async_queue = AsyncFQ(config)
            sync_queue = FQ(config)

            await async_queue.initialize()
            sync_queue.initialize()
            await async_queue._r.flushdb()

            try:
                sync_job_id = self._job_id()
                sync_queue.enqueue(
                    payload={"source": "sync"},
                    interval=1,
                    job_id=sync_job_id,
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                )
                sync_job = await async_queue.dequeue(queue_type=self.queue_type)
                self.assertEqual(sync_job["status"], "success")
                self.assertEqual(
                    set(sync_job),
                    {
                        "status",
                        "queue_id",
                        "job_id",
                        "payload",
                        "requeues_remaining",
                    },
                )
                self.assertEqual(sync_job["job_id"], sync_job_id)
                self.assertEqual(sync_job["payload"], {"source": "sync"})
                self.assertEqual(
                    await async_queue.finish(
                        queue_type=self.queue_type,
                        queue_id=sync_job["queue_id"],
                        job_id=sync_job["job_id"],
                    ),
                    {"status": "success"},
                )

                async_job_id = self._job_id()
                await async_queue.enqueue(
                    payload={"source": "async"},
                    interval=1,
                    job_id=async_job_id,
                    queue_id=self.queue_id,
                    queue_type=self.queue_type,
                )
                await asyncio.sleep(0.01)
                async_job = sync_queue.dequeue(queue_type=self.queue_type)
                self.assertEqual(async_job["status"], "success")
                self.assertEqual(
                    set(async_job),
                    {
                        "status",
                        "queue_id",
                        "job_id",
                        "payload",
                        "requeues_remaining",
                    },
                )
                self.assertEqual(async_job["job_id"], async_job_id)
                self.assertEqual(async_job["payload"], {"source": "async"})
                self.assertEqual(
                    sync_queue.finish(
                        queue_type=self.queue_type,
                        queue_id=async_job["queue_id"],
                        job_id=async_job["job_id"],
                    ),
                    {"status": "success"},
                )
            finally:
                sync_queue._r.flushdb()
                sync_queue.close()
                await async_queue.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
