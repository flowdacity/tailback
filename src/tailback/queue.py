# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
#  Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

import asyncio

from tailback.base import BaseTailback
from tailback.lua import LuaScripts
from tailback.redis import create_async_redis_client, validate_async_redis_connection


class Tailback(BaseTailback):
    """Async Tailback API."""

    async def initialize(self):
        """Set up the async Redis client and register Lua scripts."""
        self._r = create_async_redis_client(self.config.redis)
        await validate_async_redis_connection(self._r)
        self._register_lua_scripts()

    def _register_lua_scripts(self):
        self._scripts = LuaScripts.register(self._r)

    def reload_lua_scripts(self):
        """Lets user reload the Lua scripts at run time."""
        self._register_lua_scripts()

    async def enqueue(
        self,
        payload,
        interval,
        job_id,
        queue_id,
        queue_type="default",
        requeue_limit=None,
    ):
        """Enqueue a job into the specified queue_id and queue_type."""
        keys, args = self._build_enqueue_call(
            payload,
            interval,
            job_id,
            queue_id,
            queue_type,
            requeue_limit,
        )
        await self._scripts.enqueue(keys=keys, args=args)
        return {"status": "queued"}

    async def dequeue(self, queue_type="default"):
        """Dequeue a ready job for queue_type, or return failure."""
        keys, args = self._build_dequeue_call(queue_type)
        dequeue_response = await self._scripts.dequeue(keys=keys, args=args)
        return self._dequeue_response(dequeue_response)

    async def finish(self, job_id, queue_id, queue_type="default"):
        """Mark a dequeued job as completed successfully."""
        keys, args = self._build_finish_call(job_id, queue_id, queue_type)
        finish_response = await self._scripts.finish(keys=keys, args=args)
        return self._finish_response(finish_response)

    async def interval(self, interval, queue_id, queue_type="default"):
        """Update the interval for a queue_id and queue_type."""
        keys, args = self._build_interval_call(interval, queue_id, queue_type)
        interval_response = await self._scripts.interval(keys=keys, args=args)
        return self._interval_response(interval_response)

    async def requeue(self):
        """Re-queue expired active jobs back into their ready queues."""
        timestamp = self._current_timestamp()
        active_queue_type_list = await self._r.smembers(self._keys.active_queue_types)
        for queue_type in active_queue_type_list:
            queue_type = self._decode_redis_value(queue_type)
            keys, args = self._build_requeue_call(queue_type, timestamp)
            job_discard_list = await self._scripts.requeue(keys=keys, args=args)
            for job in job_discard_list:
                queue_id, job_id = self._decode_requeue_job(job)
                await self.finish(
                    job_id=job_id,
                    queue_id=queue_id,
                    queue_type=queue_type,
                )

    async def metrics(self, queue_type=None, queue_id=None):
        """Return global, queue-type, or queue-specific metrics."""
        self._validate_metrics_call(queue_type, queue_id)

        if not queue_type and not queue_id:
            active_queue_types = await self._r.smembers(self._keys.active_queue_types)
            ready_queue_types = await self._r.smembers(self._keys.ready_queue_types)

            keys, args = self._build_global_metrics_call()
            enqueue_details, dequeue_details = await self._scripts.metrics(
                keys=keys,
                args=args,
            )
            return self._global_metrics_response(
                active_queue_types,
                ready_queue_types,
                enqueue_details,
                dequeue_details,
            )

        if queue_type and not queue_id:
            ready_queue_key, active_queue_key = self._queue_type_metrics_keys(
                queue_type
            )
            pipe = self._r.pipeline()
            pipe.zrange(ready_queue_key, 0, -1)
            pipe.zrange(active_queue_key, 0, -1)
            ready_queues, active_queues = await pipe.execute()
            return self._queue_type_metrics_response(ready_queues, active_queues)

        if queue_type and queue_id:
            keys, args = self._build_queue_metrics_call(queue_type, queue_id)
            enqueue_details, dequeue_details = await self._scripts.metrics(
                keys=keys,
                args=args,
            )
            queue_length = await self._r.llen(
                self._queue_length_key(queue_type, queue_id)
            )
            return self._queue_metrics_response(
                queue_length,
                enqueue_details,
                dequeue_details,
            )

        return {"status": "failure"}

    async def deep_status(self):
        """
        Check Redis availability. If Redis is down, set() will raise.
        :return: value or None
        """
        return await self._r.set(self._keys.deep_status, "sharq_deep_status")

    async def clear_queue(self, queue_type=None, queue_id=None, purge_all=False):
        """Clear entries in a queue and optionally purge related resources."""
        plan = self._clear_queue_plan(queue_type, queue_id)

        response = self._clear_queue_empty_response()
        queued_status = await self._r.zrem(plan.primary_set, queue_id)
        if queued_status:
            response = self._clear_queue_removed_response()

        if queued_status and purge_all:
            job_list = await self._r.lrange(plan.job_queue, 0, -1)
            pipe = self._r.pipeline()
            for job_uuid in job_list:
                if job_uuid is None:
                    continue
                job_uuid = self._decode_redis_value(job_uuid)
                pipe.hdel(plan.payload_hash, plan.payload_member(job_uuid))

            pipe.hdel(plan.interval_hash, plan.interval_member)
            pipe.delete(plan.job_queue)
            await pipe.execute()
            return self._clear_queue_purged_response()

        await self._r.delete(plan.job_queue)
        return response

    async def get_queue_length(self, queue_type, queue_id):
        """
        Return the current Redis list length for key_prefix:queue_type:queue_id.
        """
        redis_key = self._queue_length_key(queue_type, queue_id)
        return await self._r.llen(redis_key)

    async def close(self):
        """Cleanly close the underlying Redis client or connection pool."""
        if self._r is None:
            return

        conn = self._r

        aclose = getattr(conn, "aclose", None)
        if callable(aclose):
            await aclose()
            self._r = None
            return

        close = getattr(conn, "close", None)
        if callable(close):
            maybe_coro = close()
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro

        wait_closed = getattr(conn, "wait_closed", None)
        if callable(wait_closed):
            maybe_coro = wait_closed()
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro

        pool = getattr(conn, "connection_pool", None)
        if pool is not None:
            disconnect = getattr(pool, "disconnect", None)
            if callable(disconnect):
                maybe_coro = disconnect()
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro

        self._r = None
