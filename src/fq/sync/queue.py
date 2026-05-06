# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from fq.config import FQConfig
from fq.exceptions import BadArgumentException
from fq.keys import RedisKeys
from fq.lua import Lua
from fq.redis import create_sync_redis_client, validate_sync_redis_connection
from fq.responses import (
    decode_redis_value,
    format_dequeue_response,
    format_metrics_counts,
    format_queue_ids,
    format_queue_types,
)
from fq.utils import generate_epoch
from fq.validators import (
    validate_clear_queue_arguments,
    validate_dequeue_arguments,
    validate_enqueue_arguments,
    validate_finish_arguments,
    validate_get_queue_length_arguments,
    validate_interval_arguments,
    validate_metrics_arguments,
)


class FQ(object):
    """Synchronous Flowdacity Queue API."""

    def __init__(self, config):
        self._r = None
        self._scripts = None
        self.config = FQConfig.from_mapping(config)
        self._keys = RedisKeys(self.config.redis.key_prefix)

        self._key_prefix = self.config.redis.key_prefix
        self._job_expire_interval = int(self.config.job_expire_interval)
        self._default_job_requeue_limit = int(
            self.config.default_job_requeue_limit
        )

    def initialize(self):
        """Set up the synchronous Redis client and register Lua scripts."""
        self._r = create_sync_redis_client(self.config.redis)
        validate_sync_redis_connection(self._r)
        self._register_lua_scripts()

    def redis_client(self):
        return self._r

    def _register_lua_scripts(self):
        self._scripts = Lua.register(self._r)

    def reload_lua_scripts(self):
        """Lets user reload the Lua scripts at run time."""
        self._register_lua_scripts()

    def enqueue(
        self,
        payload,
        interval,
        job_id,
        queue_id,
        queue_type="default",
        requeue_limit=None,
    ):
        """Enqueue a job into the specified queue_id and queue_type."""
        enqueue_args = validate_enqueue_arguments(
            payload,
            interval,
            job_id,
            queue_id,
            queue_type,
            requeue_limit,
            self._default_job_requeue_limit,
        )

        keys = [self._key_prefix, queue_type]
        args = [
            str(generate_epoch()),
            queue_id,
            job_id,
            enqueue_args.serialized_payload,
            interval,
            enqueue_args.requeue_limit,
        ]
        self._scripts.enqueue(keys=keys, args=args)
        return {"status": "queued"}

    def dequeue(self, queue_type="default"):
        """Dequeue a ready job for queue_type, or return failure."""
        validate_dequeue_arguments(queue_type)

        keys = [self._key_prefix, queue_type]
        args = [str(generate_epoch()), self._job_expire_interval]

        dequeue_response = self._scripts.dequeue(keys=keys, args=args)
        return format_dequeue_response(dequeue_response)

    def finish(self, job_id, queue_id, queue_type="default"):
        """Mark a dequeued job as completed successfully."""
        validate_finish_arguments(job_id, queue_id, queue_type)

        keys = [self._key_prefix, queue_type]
        args = [queue_id, job_id]

        finish_response = self._scripts.finish(keys=keys, args=args)
        if finish_response == 0:
            return {"status": "failure"}

        return {"status": "success"}

    def interval(self, interval, queue_id, queue_type="default"):
        """Update the interval for a queue_id and queue_type."""
        validate_interval_arguments(interval, queue_id, queue_type)

        keys = [
            self._keys.interval_hash,
            self._keys.interval_member(queue_type, queue_id),
        ]
        args = [interval]

        interval_response = self._scripts.interval(keys=keys, args=args)
        if interval_response == 0:
            return {"status": "failure"}

        return {"status": "success"}

    def requeue(self):
        """Re-queue expired active jobs back into their ready queues."""
        timestamp = str(generate_epoch())
        active_queue_type_list = self._r.smembers(self._keys.active_queue_types)
        for queue_type in active_queue_type_list:
            queue_type = decode_redis_value(queue_type)
            keys = [self._key_prefix, queue_type]
            args = [timestamp]
            job_discard_list = self._scripts.requeue(keys=keys, args=args)
            for job in job_discard_list:
                queue_id, job_id = decode_redis_value(job).split(":")
                self.finish(job_id=job_id, queue_id=queue_id, queue_type=queue_type)

    def metrics(self, queue_type=None, queue_id=None):
        """Return global, queue-type, or queue-specific metrics."""
        validate_metrics_arguments(queue_type, queue_id)

        response = {"status": "failure"}
        if not queue_type and not queue_id:
            active_queue_types = self._r.smembers(self._keys.active_queue_types)
            ready_queue_types = self._r.smembers(self._keys.ready_queue_types)
            queue_types = format_queue_types(active_queue_types, ready_queue_types)

            keys = [self._key_prefix]
            args = [str(generate_epoch())]
            enqueue_details, dequeue_details = self._scripts.metrics(
                keys=keys,
                args=args,
            )
            enqueue_counts, dequeue_counts = format_metrics_counts(
                enqueue_details,
                dequeue_details,
            )

            response.update(
                {
                    "status": "success",
                    "queue_types": queue_types,
                    "enqueue_counts": enqueue_counts,
                    "dequeue_counts": dequeue_counts,
                }
            )
            return response

        if queue_type and not queue_id:
            pipe = self._r.pipeline()
            pipe.zrange(self._keys.ready_queue_set(queue_type), 0, -1)
            pipe.zrange(self._keys.active_queue_set(queue_type), 0, -1)
            ready_queues, active_queues = pipe.execute()
            queue_list = format_queue_ids(ready_queues, active_queues)
            response.update({"status": "success", "queue_ids": queue_list})
            return response

        if queue_type and queue_id:
            keys = [self._keys.job_queue(queue_type, queue_id)]
            args = [str(generate_epoch())]
            enqueue_details, dequeue_details = self._scripts.metrics(
                keys=keys,
                args=args,
            )
            enqueue_counts, dequeue_counts = format_metrics_counts(
                enqueue_details,
                dequeue_details,
            )
            queue_length = self._r.llen(self._keys.job_queue(queue_type, queue_id))

            response.update(
                {
                    "status": "success",
                    "queue_length": int(queue_length),
                    "enqueue_counts": enqueue_counts,
                    "dequeue_counts": dequeue_counts,
                }
            )
            return response

        if not queue_type and queue_id:
            raise BadArgumentException(
                "`queue_id` should be accompanied by `queue_type`."
            )

        return response

    def deep_status(self):
        """
        Check Redis availability. If Redis is down, set() will raise.
        :return: value or None
        """
        return self._r.set(self._keys.deep_status, "sharq_deep_status")

    def clear_queue(self, queue_type=None, queue_id=None, purge_all=False):
        """Clear entries in a queue and optionally purge related resources."""
        validate_clear_queue_arguments(queue_type, queue_id)

        response = {"status": "Failure", "message": "No queued calls found"}
        primary_set = self._keys.ready_queue_set(queue_type)
        queued_status = self._r.zrem(primary_set, queue_id)
        if queued_status:
            response.update(
                {
                    "status": "Success",
                    "message": "Successfully removed all queued calls",
                }
            )

        job_queue_list = self._keys.job_queue(queue_type, queue_id)
        if queued_status and purge_all:
            job_list = self._r.lrange(job_queue_list, 0, -1)
            pipe = self._r.pipeline()
            for job_uuid in job_list:
                if job_uuid is None:
                    continue
                job_uuid_str = decode_redis_value(job_uuid)
                pipe.hdel(
                    self._keys.payload_hash,
                    self._keys.payload_member(queue_type, queue_id, job_uuid_str),
                )

            pipe.hdel(
                self._keys.interval_hash,
                self._keys.interval_member(queue_type, queue_id),
            )
            pipe.delete(job_queue_list)
            pipe.execute()
            response.update(
                {
                    "status": "Success",
                    "message": "Successfully removed all queued calls and purged related resources",
                }
            )
        else:
            self._r.delete(job_queue_list)

        return response

    def get_queue_length(self, queue_type, queue_id):
        """
        Return the current Redis list length for key_prefix:queue_type:queue_id.
        """
        validate_get_queue_length_arguments(queue_type, queue_id)
        return self._r.llen(self._keys.job_queue(queue_type, queue_id))

    def close(self):
        """Close the underlying synchronous Redis client."""
        if self._r is None:
            return

        conn = self._r
        close = getattr(conn, "close", None)
        if callable(close):
            close()
            self._r = None
            return

        pool = getattr(conn, "connection_pool", None)
        if pool is not None:
            disconnect = getattr(pool, "disconnect", None)
            if callable(disconnect):
                disconnect()

        self._r = None
