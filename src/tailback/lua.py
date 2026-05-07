# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LuaScripts:
    enqueue: Any
    dequeue: Any
    finish: Any
    interval: Any
    requeue: Any
    metrics: Any

    @classmethod
    def register(cls, redis_client):
        registered_scripts = {
            script_field.name: redis_client.register_script(
                cls._read_script(script_field.name)
            )
            for script_field in fields(cls)
        }
        return cls(**registered_scripts)

    @staticmethod
    def _read_script(script_name):
        script_path = (
            Path(__file__).with_name("scripts") / "lua" / ("%s.lua" % script_name)
        )
        return script_path.read_text(encoding="utf-8")
