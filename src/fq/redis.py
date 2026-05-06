# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from redis import Redis as SyncRedis
from redis import RedisCluster as SyncRedisCluster
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from fq.exceptions import FQException


def create_async_redis_client(redis_config):
    if redis_config.conn_type == "unix_sock":
        return AsyncRedis(
            db=redis_config.db,
            unix_socket_path=redis_config.unix_socket_path,
        )

    if redis_config.conn_type == "tcp_sock":
        if redis_config.clustered:
            startup_nodes = [
                {
                    "host": redis_config.host,
                    "port": int(redis_config.port),
                }
            ]
            return AsyncRedisCluster(
                startup_nodes=startup_nodes,
                decode_responses=False,
                socket_timeout=5,
            )

        return AsyncRedis(
            db=redis_config.db,
            host=redis_config.host,
            port=int(redis_config.port),
            password=redis_config.password,
        )

    raise FQException("Unknown redis conn_type: %s" % redis_config.conn_type)


def create_sync_redis_client(redis_config):
    if redis_config.conn_type == "unix_sock":
        return SyncRedis(
            db=redis_config.db,
            unix_socket_path=redis_config.unix_socket_path,
        )

    if redis_config.conn_type == "tcp_sock":
        if redis_config.clustered:
            return SyncRedisCluster(
                host=redis_config.host,
                port=int(redis_config.port),
                decode_responses=False,
                socket_timeout=5,
            )

        return SyncRedis(
            db=redis_config.db,
            host=redis_config.host,
            port=int(redis_config.port),
            password=redis_config.password,
        )

    raise FQException("Unknown redis conn_type: %s" % redis_config.conn_type)


async def validate_async_redis_connection(redis_client):
    if redis_client is None:
        raise FQException("Redis client is not initialized")

    ping = getattr(redis_client, "ping", None)
    if not callable(ping):
        return

    try:
        result = await ping()
    except Exception as exc:
        raise FQException("Failed to connect to Redis: %s" % exc) from exc

    if result is False:
        raise FQException("Failed to connect to Redis: ping returned False")


def validate_sync_redis_connection(redis_client):
    if redis_client is None:
        raise FQException("Redis client is not initialized")

    ping = getattr(redis_client, "ping", None)
    if not callable(ping):
        return

    try:
        result = ping()
    except Exception as exc:
        raise FQException("Failed to connect to Redis: %s" % exc) from exc

    if result is False:
        raise FQException("Failed to connect to Redis: ping returned False")
