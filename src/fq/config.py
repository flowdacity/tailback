# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from collections.abc import Mapping
from dataclasses import dataclass

from fq.exceptions import FQException
from fq.utils import is_valid_interval, is_valid_requeue_limit


REDIS_CONN_TYPES = {"tcp_sock", "unix_sock"}


@dataclass(frozen=True)
class RedisConfig:
    key_prefix: str
    conn_type: str
    db: int
    host: str | None = None
    port: int | None = None
    unix_socket_path: str | None = None
    clustered: bool = False
    password: str | None = None


@dataclass(frozen=True)
class FQConfig:
    redis: RedisConfig
    job_expire_interval: int
    job_requeue_interval: int
    default_job_requeue_limit: int

    @classmethod
    def from_mapping(cls, config):
        normalized = _normalize_config_sections(config)
        _require_config_sections(normalized)

        redis_config = normalized["redis"]
        fq_config = normalized["fq"]

        _validate_redis_config(redis_config)
        _validate_fq_config(fq_config)
        _validate_connection_config(redis_config)
        _validate_optional_redis_config(redis_config)

        return cls(
            redis=RedisConfig(
                key_prefix=redis_config["key_prefix"],
                conn_type=redis_config["conn_type"],
                db=redis_config["db"],
                host=redis_config.get("host"),
                port=redis_config.get("port"),
                unix_socket_path=redis_config.get("unix_socket_path"),
                clustered=redis_config.get("clustered", False),
                password=redis_config.get("password"),
            ),
            job_expire_interval=fq_config["job_expire_interval"],
            job_requeue_interval=fq_config["job_requeue_interval"],
            default_job_requeue_limit=fq_config["default_job_requeue_limit"],
        )


def _normalize_config_sections(config):
    if not isinstance(config, Mapping):
        raise FQException("Config must be a mapping with redis and fq sections")

    normalized = {}
    for section_name, section_values in config.items():
        if not isinstance(section_values, Mapping):
            raise FQException("Config section '%s' must be a mapping" % section_name)

        normalized[str(section_name)] = {
            str(option): value for option, value in section_values.items()
        }

    return normalized


def _require_config_sections(config):
    if "redis" not in config or "fq" not in config:
        raise FQException("Config missing required sections: redis, fq")


def _require_config_value(config, section_name, option_name):
    if option_name not in config:
        raise FQException("Missing config: %s.%s" % (section_name, option_name))

    return config[option_name]


def _is_non_empty_string(value):
    return isinstance(value, str) and bool(value)


def _is_int_not_bool(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_redis_config(redis_config):
    key_prefix = _require_config_value(redis_config, "redis", "key_prefix")
    if not _is_non_empty_string(key_prefix):
        raise FQException("Invalid config: redis.key_prefix must be a non-empty string")

    conn_type = _require_config_value(redis_config, "redis", "conn_type")
    if conn_type not in REDIS_CONN_TYPES:
        raise FQException(
            "Invalid config: redis.conn_type must be 'tcp_sock' or 'unix_sock'"
        )

    db = _require_config_value(redis_config, "redis", "db")
    if not _is_int_not_bool(db):
        raise FQException("Invalid config: redis.db must be an integer")


def _validate_fq_config(fq_config):
    for option_name in ("job_expire_interval", "job_requeue_interval"):
        value = _require_config_value(fq_config, "fq", option_name)
        if not is_valid_interval(value):
            raise FQException(
                "Invalid config: fq.%s must be a positive integer" % option_name
            )

    default_requeue_limit = _require_config_value(
        fq_config, "fq", "default_job_requeue_limit"
    )
    if not is_valid_requeue_limit(default_requeue_limit):
        raise FQException(
            "Invalid config: fq.default_job_requeue_limit must be an integer >= -1"
        )


def _validate_connection_config(redis_config):
    if redis_config["conn_type"] == "unix_sock":
        _validate_unix_socket_config(redis_config)
        return

    _validate_tcp_socket_config(redis_config)


def _validate_unix_socket_config(redis_config):
    unix_socket_path = _require_config_value(redis_config, "redis", "unix_socket_path")
    if not _is_non_empty_string(unix_socket_path):
        raise FQException(
            "Invalid config: redis.unix_socket_path must be a non-empty string"
        )


def _validate_tcp_socket_config(redis_config):
    host = _require_config_value(redis_config, "redis", "host")
    if not _is_non_empty_string(host):
        raise FQException("Invalid config: redis.host must be a non-empty string")

    port = _require_config_value(redis_config, "redis", "port")
    if not _is_int_not_bool(port):
        raise FQException("Invalid config: redis.port must be an integer")

    if "clustered" in redis_config and not isinstance(redis_config["clustered"], bool):
        raise FQException("Invalid config: redis.clustered must be a boolean")


def _validate_optional_redis_config(redis_config):
    if "password" in redis_config and redis_config["password"] is not None:
        if not isinstance(redis_config["password"], str):
            raise FQException("Invalid config: redis.password must be a string")
