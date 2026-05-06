# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from dataclasses import dataclass
from pathlib import Path


LUA_SCRIPT_NAMES = ("enqueue", "dequeue", "finish", "interval", "requeue", "metrics")


@dataclass(frozen=True)
class Lua:
    enqueue: object
    dequeue: object
    finish: object
    interval: object
    requeue: object
    metrics: object

    @classmethod
    def register(cls, redis_client):
        registered_scripts = {
            script_name: redis_client.register_script(cls._read_script(script_name))
            for script_name in LUA_SCRIPT_NAMES
        }
        return cls(**registered_scripts)

    @staticmethod
    def _read_script(script_name):
        script_path = (
            Path(__file__).with_name("scripts") / "lua" / ("%s.lua" % script_name)
        )
        return script_path.read_text(encoding="utf-8")
