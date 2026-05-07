# -*- coding: utf-8 -*-

from copy import deepcopy
from os.path import join
from tempfile import gettempdir


TEST_UNIX_SOCKET_PATH = join(gettempdir(), "redis.sock")


TEST_CONFIG = {
    "queue": {
        "key_prefix": "test_tailback",
        "job_expire_interval": 5000,
        "job_requeue_interval": 5000,
        "default_job_requeue_limit": -1,
    },
    "redis": {
        "db": 0,
        "conn_type": "tcp_sock",
        "unix_socket_path": TEST_UNIX_SOCKET_PATH,
        "port": 6379,
        "host": "127.0.0.1",
        "clustered": False,
        "password": "",
    },
}


def build_test_config(**section_overrides):
    config = deepcopy(TEST_CONFIG)
    for section_name, overrides in section_overrides.items():
        config.setdefault(section_name, {})
        config[section_name].update(overrides)
    return config
