# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from redis import Redis, RedisCluster

from fq.core import (
    decode_redis_value,
    enqueue_script_args,
    format_dequeue_response,
    format_metrics_counts,
    format_queue_ids,
    format_queue_types,
    generate_epoch,
    load_lua_scripts,
    normalize_config,
    validate_clear_queue_arguments,
    validate_dequeue_arguments,
    validate_enqueue_arguments,
    validate_finish_arguments,
    validate_get_queue_length_arguments,
    validate_interval_arguments,
    validate_metrics_arguments,
)
from fq.exceptions import BadArgumentException, FQException


class FQ(object):
    """Synchronous FQ API backed by redis-py's synchronous client."""

    def __init__(self, config):
        self._r = None
        self.config = normalize_config(config)

    def initialize(self):
        """Set up the synchronous Redis client and Lua scripts."""
        fq_config = self.config["fq"]
        redis_config = self.config["redis"]

        self._key_prefix = redis_config["key_prefix"]
        self._job_expire_interval = int(fq_config["job_expire_interval"])
        self._default_job_requeue_limit = int(fq_config["default_job_requeue_limit"])

        redis_connection_type = redis_config["conn_type"]
        db = redis_config["db"]

        if redis_connection_type == "unix_sock":
            self._r = Redis(
                db=db,
                unix_socket_path=redis_config["unix_socket_path"],
            )
        elif redis_connection_type == "tcp_sock":
            isclustered = False
            if "clustered" in redis_config:
                isclustered = redis_config["clustered"]

            if isclustered:
                self._r = RedisCluster(
                    host=redis_config["host"],
                    port=int(redis_config["port"]),
                    decode_responses=False,
                    socket_timeout=5,
                )
            else:
                self._r = Redis(
                    db=db,
                    host=redis_config["host"],
                    port=int(redis_config["port"]),
                    password=redis_config.get("password"),
                )
        else:
            raise FQException("Unknown redis conn_type: %s" % redis_connection_type)

        self._validate_redis_connection()
        self._load_lua_scripts()

    def _validate_redis_connection(self):
        """Ping Redis once to surface bad connection details early."""
        if self._r is None:
            raise FQException("Redis client is not initialized")

        ping = getattr(self._r, "ping", None)
        if not callable(ping):
            return

        try:
            result = ping()
        except Exception as exc:
            raise FQException("Failed to connect to Redis: %s" % exc) from exc

        if result is False:
            raise FQException("Failed to connect to Redis: ping returned False")

    def redis_client(self):
        return self._r

    def _load_lua_scripts(self):
        """Loads all Lua scripts required by FQ."""
        load_lua_scripts(self, self._r)

    def reload_lua_scripts(self):
        """Lets user reload the Lua scripts at run time."""
        self._load_lua_scripts()

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
        serialized_payload, requeue_limit = validate_enqueue_arguments(
            payload,
            interval,
            job_id,
            queue_id,
            queue_type,
            requeue_limit,
            self._default_job_requeue_limit,
        )
        keys, args = enqueue_script_args(
            self._key_prefix,
            queue_type,
            queue_id,
            job_id,
            serialized_payload,
            interval,
            requeue_limit,
        )
        self._lua_enqueue(keys=keys, args=args)
        return {"status": "queued"}

    def dequeue(self, queue_type="default"):
        """Dequeue a ready job for the queue_type, or return failure."""
        validate_dequeue_arguments(queue_type)

        timestamp = str(generate_epoch())
        keys = [self._key_prefix, queue_type]
        args = [timestamp, self._job_expire_interval]

        dequeue_response = self._lua_dequeue(keys=keys, args=args)
        return format_dequeue_response(dequeue_response)

    def finish(self, job_id, queue_id, queue_type="default"):
        """Mark a dequeued job as completed successfully."""
        validate_finish_arguments(job_id, queue_id, queue_type)

        keys = [self._key_prefix, queue_type]
        args = [queue_id, job_id]

        finish_response = self._lua_finish(keys=keys, args=args)
        if finish_response == 0:
            return {"status": "failure"}

        return {"status": "success"}

    def interval(self, interval, queue_id, queue_type="default"):
        """Update the interval for a queue_id and queue_type."""
        validate_interval_arguments(interval, queue_id, queue_type)

        interval_hmap_key = "%s:interval" % self._key_prefix
        interval_queue_key = "%s:%s" % (queue_type, queue_id)
        keys = [interval_hmap_key, interval_queue_key]
        args = [interval]

        interval_response = self._lua_interval(keys=keys, args=args)
        if interval_response == 0:
            return {"status": "failure"}
        return {"status": "success"}

    def requeue(self):
        """Re-queue expired active jobs back into their ready queues."""
        timestamp = str(generate_epoch())
        active_queue_type_list = self._r.smembers(
            "%s:active:queue_type" % self._key_prefix
        )
        for queue_type in active_queue_type_list:
            queue_type = decode_redis_value(queue_type)
            keys = [self._key_prefix, queue_type]
            args = [timestamp]
            job_discard_list = self._lua_requeue(keys=keys, args=args)
            for job in job_discard_list:
                queue_id, job_id = decode_redis_value(job).split(":")
                self.finish(job_id=job_id, queue_id=queue_id, queue_type=queue_type)

    def metrics(self, queue_type=None, queue_id=None):
        """Return global, queue-type, or queue-specific metrics."""
        validate_metrics_arguments(queue_type, queue_id)

        response = {"status": "failure"}
        if not queue_type and not queue_id:
            active_queue_types = self._r.smembers(
                "%s:active:queue_type" % self._key_prefix
            )
            ready_queue_types = self._r.smembers(
                "%s:ready:queue_type" % self._key_prefix
            )
            queue_types = format_queue_types(active_queue_types, ready_queue_types)

            timestamp = str(generate_epoch())
            keys = [self._key_prefix]
            args = [timestamp]
            enqueue_details, dequeue_details = self._lua_metrics(keys=keys, args=args)
            enqueue_counts, dequeue_counts = format_metrics_counts(
                enqueue_details, dequeue_details
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
        elif queue_type and not queue_id:
            pipe = self._r.pipeline()
            pipe.zrange("%s:%s" % (self._key_prefix, queue_type), 0, -1)
            pipe.zrange("%s:%s:active" % (self._key_prefix, queue_type), 0, -1)
            ready_queues, active_queues = pipe.execute()
            queue_list = format_queue_ids(ready_queues, active_queues)
            response.update({"status": "success", "queue_ids": queue_list})
            return response
        elif queue_type and queue_id:
            timestamp = str(generate_epoch())
            keys = ["%s:%s:%s" % (self._key_prefix, queue_type, queue_id)]
            args = [timestamp]
            enqueue_details, dequeue_details = self._lua_metrics(keys=keys, args=args)
            enqueue_counts, dequeue_counts = format_metrics_counts(
                enqueue_details, dequeue_details
            )

            queue_length = self._r.llen(
                "%s:%s:%s" % (self._key_prefix, queue_type, queue_id)
            )

            response.update(
                {
                    "status": "success",
                    "queue_length": int(queue_length),
                    "enqueue_counts": enqueue_counts,
                    "dequeue_counts": dequeue_counts,
                }
            )
            return response
        elif not queue_type and queue_id:
            raise BadArgumentException(
                "`queue_id` should be accompanied by `queue_type`."
            )

        return response

    def deep_status(self):
        """
        Check Redis availability. If Redis is down, set() will raise.
        :return: value or None
        """
        return self._r.set(
            "fq:deep_status:{}".format(self._key_prefix), "sharq_deep_status"
        )

    def clear_queue(self, queue_type=None, queue_id=None, purge_all=False):
        """Clear entries in a queue and optionally purge related resources."""
        validate_clear_queue_arguments(queue_type, queue_id)

        response = {"status": "Failure", "message": "No queued calls found"}
        primary_set = "{}:{}".format(self._key_prefix, queue_type)
        queued_status = self._r.zrem(primary_set, queue_id)
        if queued_status:
            response.update(
                {
                    "status": "Success",
                    "message": "Successfully removed all queued calls",
                }
            )

        job_queue_list = "{}:{}:{}".format(self._key_prefix, queue_type, queue_id)
        if queued_status and purge_all:
            job_list = self._r.lrange(job_queue_list, 0, -1)
            pipe = self._r.pipeline()
            for job_uuid in job_list:
                if job_uuid is None:
                    continue
                job_uuid_str = decode_redis_value(job_uuid)
                payload_set = "{}:payload".format(self._key_prefix)
                job_payload_key = "{}:{}:{}".format(queue_type, queue_id, job_uuid_str)
                pipe.hdel(payload_set, job_payload_key)

            interval_set = "{}:interval".format(self._key_prefix)
            job_interval_key = "{}:{}".format(queue_type, queue_id)
            pipe.hdel(interval_set, job_interval_key)
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

        redis_key = self._key_prefix + ":" + queue_type + ":" + queue_id
        return self._r.llen(redis_key)

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
