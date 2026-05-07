# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from collections.abc import Mapping
from dataclasses import dataclass

from tailback.exceptions import TailbackException
from tailback.utils import is_valid_interval, is_valid_requeue_limit


REDIS_CONN_TYPES = {"tcp_sock", "unix_sock"}


@dataclass(frozen=True)
class RedisConfig:
    conn_type: str
    db: int
    host: str | None = None
    port: int | None = None
    unix_socket_path: str | None = None
    clustered: bool = False
    password: str | None = None

    @classmethod
    def from_mapping(cls, config):
        cls._validate_required(config)
        cls._validate_connection(config)
        cls._validate_optional(config)

        return cls(
            conn_type=config["conn_type"],
            db=config["db"],
            host=config.get("host"),
            port=config.get("port"),
            unix_socket_path=config.get("unix_socket_path"),
            clustered=config.get("clustered", False),
            password=config.get("password"),
        )

    @classmethod
    def _validate_required(cls, config):
        conn_type = cls._require_value(config, "conn_type")
        if conn_type not in REDIS_CONN_TYPES:
            raise TailbackException(
                "Invalid config: redis.conn_type must be 'tcp_sock' or 'unix_sock'"
            )

        db = cls._require_value(config, "db")
        if not cls._is_int_not_bool(db):
            raise TailbackException("Invalid config: redis.db must be an integer")

    @classmethod
    def _validate_connection(cls, config):
        cls._validate_clustered(config)

        if config["conn_type"] == "unix_sock":
            cls._validate_unix_socket(config)
            return

        cls._validate_tcp_socket(config)

    @classmethod
    def _validate_clustered(cls, config):
        if "clustered" in config and not isinstance(config["clustered"], bool):
            raise TailbackException("Invalid config: redis.clustered must be a boolean")

    @classmethod
    def _validate_unix_socket(cls, config):
        unix_socket_path = cls._require_value(config, "unix_socket_path")
        if not cls._is_non_empty_string(unix_socket_path):
            raise TailbackException(
                "Invalid config: redis.unix_socket_path must be a non-empty string"
            )

    @classmethod
    def _validate_tcp_socket(cls, config):
        host = cls._require_value(config, "host")
        if not cls._is_non_empty_string(host):
            raise TailbackException("Invalid config: redis.host must be a non-empty string")

        port = cls._require_value(config, "port")
        if not cls._is_int_not_bool(port):
            raise TailbackException("Invalid config: redis.port must be an integer")

        if port < 1 or port > 65535:
            raise TailbackException(
                "Invalid config: redis.port must be an integer between 1 and 65535"
            )

    @classmethod
    def _validate_optional(cls, config):
        if "password" in config and config["password"] is not None:
            if not isinstance(config["password"], str):
                raise TailbackException("Invalid config: redis.password must be a string")

    @staticmethod
    def _require_value(config, option_name):
        if option_name not in config:
            raise TailbackException("Missing config: redis.%s" % option_name)

        return config[option_name]

    @staticmethod
    def _is_non_empty_string(value):
        return isinstance(value, str) and bool(value)

    @staticmethod
    def _is_int_not_bool(value):
        return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class QueueConfig:
    key_prefix: str
    job_expire_interval: int
    job_requeue_interval: int
    default_job_requeue_limit: int

    @classmethod
    def from_mapping(cls, config):
        cls._validate_required(config)

        return cls(
            key_prefix=config["key_prefix"],
            job_expire_interval=config["job_expire_interval"],
            job_requeue_interval=config["job_requeue_interval"],
            default_job_requeue_limit=config["default_job_requeue_limit"],
        )

    @classmethod
    def _validate_required(cls, config):
        key_prefix = cls._require_value(config, "key_prefix")
        if not cls._is_non_empty_string(key_prefix):
            raise TailbackException(
                "Invalid config: queue.key_prefix must be a non-empty string"
            )

        for option_name in ("job_expire_interval", "job_requeue_interval"):
            value = cls._require_value(config, option_name)
            if not is_valid_interval(value):
                raise TailbackException(
                    "Invalid config: queue.%s must be a positive integer"
                    % option_name
                )

        default_requeue_limit = cls._require_value(config, "default_job_requeue_limit")
        if not is_valid_requeue_limit(default_requeue_limit):
            raise TailbackException(
                "Invalid config: "
                "queue.default_job_requeue_limit must be an integer >= -1"
            )

    @staticmethod
    def _require_value(config, option_name):
        if option_name not in config:
            raise TailbackException("Missing config: queue.%s" % option_name)

        return config[option_name]

    @staticmethod
    def _is_non_empty_string(value):
        return isinstance(value, str) and bool(value)


@dataclass(frozen=True)
class TailbackConfig:
    redis: RedisConfig
    queue: QueueConfig

    @classmethod
    def from_mapping(cls, config):
        normalized = cls._normalize_sections(config)
        cls._require_sections(normalized)

        return cls(
            redis=RedisConfig.from_mapping(normalized["redis"]),
            queue=QueueConfig.from_mapping(normalized["queue"]),
        )

    @staticmethod
    def _normalize_sections(config):
        if not isinstance(config, Mapping):
            raise TailbackException("Config must be a mapping with redis and queue sections")

        normalized = {}
        for section_name, section_values in config.items():
            if not isinstance(section_values, Mapping):
                raise TailbackException(
                    "Config section '%s' must be a mapping" % section_name
                )

            normalized[str(section_name)] = {
                str(option): value for option, value in section_values.items()
            }

        return normalized

    @staticmethod
    def _require_sections(config):
        if "redis" not in config or "queue" not in config:
            raise TailbackException("Config missing required sections: redis, queue")
