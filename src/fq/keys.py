# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from dataclasses import dataclass


@dataclass(frozen=True)
class RedisKeys:
    key_prefix: str

    @property
    def active_queue_types(self):
        return "%s:active:queue_type" % self.key_prefix

    @property
    def ready_queue_types(self):
        return "%s:ready:queue_type" % self.key_prefix

    @property
    def interval_hash(self):
        return "%s:interval" % self.key_prefix

    @property
    def payload_hash(self):
        return "%s:payload" % self.key_prefix

    @property
    def deep_status(self):
        return "fq:deep_status:%s" % self.key_prefix

    def ready_queue_set(self, queue_type):
        return "%s:%s" % (self.key_prefix, queue_type)

    def active_queue_set(self, queue_type):
        return "%s:%s:active" % (self.key_prefix, queue_type)

    def job_queue(self, queue_type, queue_id):
        return "%s:%s:%s" % (self.key_prefix, queue_type, queue_id)

    def interval_member(self, queue_type, queue_id):
        return "%s:%s" % (queue_type, queue_id)

    def payload_member(self, queue_type, queue_id, job_id):
        return "%s:%s:%s" % (queue_type, queue_id, job_id)
