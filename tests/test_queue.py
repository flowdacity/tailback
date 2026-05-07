# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import unittest
from datetime import date

from tailback import Tailback
from tailback.exceptions import BadArgumentException
from tests.config import build_test_config


class TailbackTest(unittest.IsolatedAsyncioTestCase):
    """The TailbackTest contains test cases which validate the Tailback interface."""

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncSetUp(self):
        self.queue = Tailback(build_test_config())
        await self.queue.initialize()

        self.valid_queue_type = "5m5_qu-eue"
        self.invalid_queue_type_1 = "s!ms_queue"
        self.invalid_queue_type_2 = "s!ms queue"
        self.invalid_queue_type_3 = ""

        self.valid_queue_id = "queue_001-"
        self.invalid_queue_id_1 = "queue#002"
        self.invalid_queue_id_2 = "queue 002"
        self.invalid_queue_id_3 = ""

        self.valid_job_id = "96c82500-9f88-11e3-bb98-22000ac6964a"
        self.invalid_job_id_1 = "93 c8"
        self.invalid_job_id_2 = "93)c8"
        self.invalid_job_id_3 = ""

        self.valid_interval = 5000
        self.invalid_interval_1 = "100"
        self.invalid_interval_2 = "$#"
        self.invalid_interval_3 = ""
        self.invalid_interval_4 = 0
        self.invalid_interval_5 = -1

        self.valid_requeue_limit_1 = 5
        self.valid_requeue_limit_2 = 0
        self.valid_requeue_limit_3 = -1
        self.invalid_requeue_limit_1 = "100"
        self.invalid_requeue_limit_2 = "$#"
        self.invalid_requeue_limit_3 = ""
        self.invalid_requeue_limit_4 = -2

        self.valid_payload = {
            "phone_number": "1000000000",
            "message": "hello world",
        }
        self.invalid_payload = {
            "phone_number": "10000000000",
            "message": "summer is here!",
            "date": date.today(),  # not serializable by msgpack
        }

        # flush redis before start
        await self.queue._r.flushdb()

    # qlty-ignore(radarlint-python:python:S5899): unittest lifecycle hook.
    async def asyncTearDown(self):
        # flush redis at the end and close connection
        await self.queue._r.flushdb()
        await self.queue.close()

    # ---------- enqueue ----------

    async def test_enqueue_queue_type_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.invalid_queue_type_1,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.invalid_queue_type_2,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.invalid_queue_type_3,
            )

    async def test_enqueue_queue_id_missing(self):
        # signature error happens before coroutine is created, so no await
        with self.assertRaises(TypeError):
            self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                # queue_id is missing
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_queue_id_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.invalid_queue_id_1,
                queue_type=self.valid_queue_type,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.invalid_queue_id_2,
                queue_type=self.valid_queue_type,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.invalid_queue_id_3,
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_job_id_missing(self):
        with self.assertRaises(TypeError):
            self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                # job_id is missing
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_job_id_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`job_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.invalid_job_id_1,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`job_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.invalid_job_id_2,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 3 (empty string)
        with self.assertRaisesRegex(
            BadArgumentException, "`job_id` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.invalid_job_id_3,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_interval_missing(self):
        with self.assertRaises(TypeError):
            self.queue.enqueue(
                payload=self.valid_payload,
                # interval is missing
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_interval_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`interval` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.invalid_interval_1,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`interval` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.invalid_interval_2,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`interval` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.invalid_interval_3,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 4
        with self.assertRaisesRegex(
            BadArgumentException, "`interval` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.invalid_interval_4,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

        # type 5
        with self.assertRaisesRegex(
            BadArgumentException, "`interval` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.invalid_interval_5,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

    async def test_enqueue_requeue_limit_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`requeue_limit` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
                requeue_limit=self.invalid_requeue_limit_1,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`requeue_limit` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
                requeue_limit=self.invalid_requeue_limit_2,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`requeue_limit` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
                requeue_limit=self.invalid_requeue_limit_3,
            )

        # type 4
        with self.assertRaisesRegex(
            BadArgumentException, "`requeue_limit` has an invalid value."
        ):
            await self.queue.enqueue(
                payload=self.valid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
                requeue_limit=self.invalid_requeue_limit_4,
            )

    async def test_enqueue_cannot_serialize_payload(self):
        with self.assertRaisesRegex(
            BadArgumentException, r"can not serialize."
        ) as ctx:
            await self.queue.enqueue(
                payload=self.invalid_payload,
                interval=self.valid_interval,
                job_id=self.valid_job_id,
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )
        self.assertIsInstance(ctx.exception.__cause__, TypeError)

    async def test_enqueue_all_ok(self):
        # with a queue_type
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

        # without a queue_type (queue_type will be 'default')
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

        # with requeue_limit 1
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_1,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

        # with requeue_limit 2
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_2,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

        # with requeue_limit 3
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_3,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

        # requeue_limit missing
        response = await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
        )
        self.assertEqual(response["status"], "queued")
        response.pop("status")
        self.assertEqual(response, {})

    # ---------- dequeue ----------

    async def test_dequeue_queue_type_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.dequeue(queue_type=self.invalid_queue_type_1)

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.dequeue(queue_type=self.invalid_queue_type_2)

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.dequeue(queue_type=self.invalid_queue_type_3)

    async def test_dequeue_all_ok(self):
        # first enqueue a job
        await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
        )

        # with a queue_type
        response = await self.queue.dequeue(queue_type=self.valid_queue_type)
        self.assertEqual(response["status"], "success")
        response.pop("status")

        self.assertIn("payload", response)
        response.pop("payload")
        self.assertIn("queue_id", response)
        response.pop("queue_id")
        self.assertIn("job_id", response)
        response.pop("job_id")
        self.assertIn("requeues_remaining", response)
        response.pop("requeues_remaining")
        self.assertEqual(response, {})

        # enqueue another job w/o queue_type (default)
        await self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
        )

        # without a queue_type
        response = await self.queue.dequeue()
        self.assertEqual(response["status"], "success")
        response.pop("status")

        self.assertIn("payload", response)
        response.pop("payload")
        self.assertIn("queue_id", response)
        response.pop("queue_id")
        self.assertIn("job_id", response)
        response.pop("job_id")
        self.assertIn("requeues_remaining", response)
        response.pop("requeues_remaining")
        self.assertEqual(response, {})

    # ---------- finish ----------

    async def test_finish_queue_type_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.invalid_queue_type_1,
                queue_id=self.valid_queue_id,
                job_id=self.valid_job_id,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.invalid_queue_type_2,
                queue_id=self.valid_queue_id,
                job_id=self.valid_job_id,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_type` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.invalid_queue_type_3,
                queue_id=self.valid_queue_id,
                job_id=self.valid_job_id,
            )

    async def test_finish_queue_id_missing(self):
        with self.assertRaises(TypeError):
            self.queue.finish(
                queue_type=self.valid_queue_type,
                # queue_id missing
                job_id=self.valid_job_id,
            )

    async def test_finish_queue_id_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.invalid_queue_id_1,
                job_id=self.valid_job_id,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.invalid_queue_id_2,
                job_id=self.valid_job_id,
            )

        # type 3
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.invalid_queue_id_3,
                job_id=self.valid_job_id,
            )

    async def test_finish_job_id_missing(self):
        with self.assertRaises(TypeError):
            self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.valid_queue_id,
                # job_id missing
            )

    async def test_finish_job_id_invalid(self):
        # type 1
        with self.assertRaisesRegex(
            BadArgumentException, "`job_id` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.valid_queue_id,
                job_id=self.invalid_job_id_1,
            )

        # type 2
        with self.assertRaisesRegex(
            BadArgumentException, "`job_id` has an invalid value."
        ):
            await self.queue.finish(
                queue_type=self.valid_queue_type,
                queue_id=self.valid_queue_id,
                job_id=self.invalid_job_id_2,
            )

    async def test_finish_all_ok(self):
        # with a queue_type, non-existent job
        response = await self.queue.finish(
            queue_type=self.valid_queue_type,
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id,
        )
        self.assertEqual(response["status"], "failure")
        response.pop("status")
        self.assertEqual(response, {})

        # without a queue_type
        response = await self.queue.finish(
            queue_id=self.valid_queue_id, job_id=self.valid_job_id
        )
        self.assertEqual(response["status"], "failure")
        response.pop("status")
        self.assertEqual(response, {})

    # ---------- interval ----------

    async def test_interval_interval_invalid(self):
        for invalid in (
            self.invalid_interval_1,
            self.invalid_interval_2,
            self.invalid_interval_3,
            self.invalid_interval_4,
            self.invalid_interval_5,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`interval` has an invalid value."
            ):
                await self.queue.interval(
                    interval=invalid,
                    queue_id=self.valid_queue_id,
                    queue_type=self.valid_queue_type,
                )

    async def test_interval_interval_missing(self):
        with self.assertRaises(TypeError):
            self.queue.interval(
                # interval missing
                queue_id=self.valid_queue_id,
                queue_type=self.valid_queue_type,
            )

    async def test_interval_invalid_queue_id(self):
        for invalid in (
            self.invalid_queue_id_1,
            self.invalid_queue_id_2,
            self.invalid_queue_id_3,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_id` has an invalid value."
            ):
                await self.queue.interval(
                    interval=self.valid_interval,
                    queue_id=invalid,
                    queue_type=self.valid_queue_type,
                )

    async def test_interval_queue_id_missing(self):
        with self.assertRaises(TypeError):
            self.queue.interval(
                interval=self.valid_interval,
                # queue_id missing
                queue_type=self.valid_queue_type,
            )

    async def test_interval_invalid_queue_type(self):
        for invalid in (
            self.invalid_queue_type_1,
            self.invalid_queue_type_2,
            self.invalid_queue_type_3,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_type` has an invalid value."
            ):
                await self.queue.interval(
                    interval=self.valid_interval,
                    queue_id=self.valid_queue_id,
                    queue_type=invalid,
                )

    async def test_interval_all_ok(self):
        # with a queue_type: no queues yet → failure
        response = await self.queue.interval(
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
        )
        self.assertEqual(response["status"], "failure")
        response.pop("status")
        self.assertEqual(response, {})

        # without a queue_type: still failure
        response = await self.queue.interval(
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
        )
        self.assertEqual(response["status"], "failure")
        response.pop("status")
        self.assertEqual(response, {})

    # ---------- metrics ----------

    async def test_metrics_no_argument(self):
        response = await self.queue.metrics()
        self.assertEqual(response["status"], "success")
        response.pop("status")

        self.assertIn("queue_types", response)
        response.pop("queue_types")
        self.assertIn("enqueue_counts", response)
        response.pop("enqueue_counts")
        self.assertIn("dequeue_counts", response)
        response.pop("dequeue_counts")

        self.assertEqual(response, {})

    async def test_metrics_only_queue_id(self):
        with self.assertRaisesRegex(
            BadArgumentException, "`queue_id` should be accompanied by `queue_type`."
        ):
            await self.queue.metrics(queue_id=self.valid_queue_id)

    async def test_metrics_only_queue_type(self):
        response = await self.queue.metrics(queue_type=self.valid_queue_type)
        self.assertEqual(response["status"], "success")
        response.pop("status")
        self.assertIn("queue_ids", response)
        response.pop("queue_ids")
        self.assertEqual(response, {})

    async def test_metrics_both_queue_id_queue_type(self):
        response = await self.queue.metrics(
            queue_type=self.valid_queue_type, queue_id=self.valid_queue_id
        )
        self.assertEqual(response["status"], "success")
        response.pop("status")

        self.assertIn("queue_length", response)
        response.pop("queue_length")
        self.assertIn("enqueue_counts", response)
        response.pop("enqueue_counts")
        self.assertIn("dequeue_counts", response)
        response.pop("dequeue_counts")

        self.assertEqual(response, {})

    async def test_metrics_queue_id_invalid(self):
        for invalid in (
            self.invalid_queue_id_1,
            self.invalid_queue_id_2,
            self.invalid_queue_id_3,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_id` has an invalid value."
            ):
                await self.queue.metrics(
                    queue_type=self.valid_queue_type,
                    queue_id=invalid,
                )

    async def test_metrics_invalid_queue_type(self):
        for invalid in (
            self.invalid_queue_type_1,
            self.invalid_queue_type_2,
            self.invalid_queue_type_3,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_type` has an invalid value."
            ):
                await self.queue.metrics(
                    queue_type=invalid,
                    queue_id=self.valid_queue_id,
                )

    # ---------- clear_queue ----------

    async def test_clear_queue_invalid_queue_type(self):
        for invalid in (
            self.invalid_queue_type_1,
            self.invalid_queue_type_2,
            self.invalid_queue_type_3,
            None,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_type` has an invalid value."
            ):
                await self.queue.clear_queue(
                    queue_type=invalid,
                    queue_id=self.valid_queue_id,
                )

    async def test_clear_queue_invalid_queue_id_(self):
        for invalid in (
            self.invalid_queue_id_1,
            self.invalid_queue_id_2,
            self.invalid_queue_id_3,
            None,
        ):
            with self.assertRaisesRegex(
                BadArgumentException, "`queue_id` has an invalid value."
            ):
                await self.queue.clear_queue(
                    queue_id=invalid,
                    queue_type=self.valid_queue_type,
                )

    # ---------- deep_status / ping ----------

    async def test_ping_redis(self):
        # using deep_status as the async health check
        res = await self.queue.deep_status()
        # deep_status sets a key; we just need that it didn't throw
        self.assertIsNotNone(res)


def main():
    unittest.main()


if __name__ == "__main__":
    main()
