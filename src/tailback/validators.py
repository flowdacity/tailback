# -*- coding: utf-8 -*-
# Copyright (c) 2025 Flowdacity Development Team. See LICENSE.txt for details.

from dataclasses import dataclass

from tailback.exceptions import BadArgumentException
from tailback.utils import (
    is_valid_identifier,
    is_valid_interval,
    is_valid_requeue_limit,
    serialize_payload,
)


INVALID_INTERVAL = "`interval` has an invalid value."
INVALID_JOB_ID = "`job_id` has an invalid value."
INVALID_QUEUE_ID = "`queue_id` has an invalid value."
INVALID_QUEUE_TYPE = "`queue_type` has an invalid value."
INVALID_REQUEUE_LIMIT = "`requeue_limit` has an invalid value."


@dataclass(frozen=True)
class EnqueueArguments:
    serialized_payload: bytes
    requeue_limit: int


def validate_enqueue_arguments(
    payload,
    interval,
    job_id,
    queue_id,
    queue_type,
    requeue_limit,
    default_requeue_limit,
):
    if not is_valid_interval(interval):
        raise BadArgumentException(INVALID_INTERVAL)

    _validate_identifier(job_id, INVALID_JOB_ID)
    _validate_identifier(queue_id, INVALID_QUEUE_ID)
    _validate_identifier(queue_type, INVALID_QUEUE_TYPE)

    if requeue_limit is None:
        requeue_limit = default_requeue_limit

    if not is_valid_requeue_limit(requeue_limit):
        raise BadArgumentException(INVALID_REQUEUE_LIMIT)

    try:
        serialized_payload = serialize_payload(payload)
    except TypeError as exc:
        raise BadArgumentException("can not serialize.") from exc

    return EnqueueArguments(
        serialized_payload=serialized_payload,
        requeue_limit=requeue_limit,
    )


def validate_dequeue_arguments(queue_type):
    _validate_identifier(queue_type, INVALID_QUEUE_TYPE)


def validate_finish_arguments(job_id, queue_id, queue_type):
    _validate_identifier(job_id, INVALID_JOB_ID)
    _validate_identifier(queue_id, INVALID_QUEUE_ID)
    _validate_identifier(queue_type, INVALID_QUEUE_TYPE)


def validate_interval_arguments(interval, queue_id, queue_type):
    if not is_valid_interval(interval):
        raise BadArgumentException(INVALID_INTERVAL)

    _validate_identifier(queue_id, INVALID_QUEUE_ID)
    _validate_identifier(queue_type, INVALID_QUEUE_TYPE)


def validate_metrics_arguments(queue_type, queue_id):
    if queue_id is not None and not is_valid_identifier(queue_id):
        raise BadArgumentException(INVALID_QUEUE_ID)

    if queue_type is not None and not is_valid_identifier(queue_type):
        raise BadArgumentException(INVALID_QUEUE_TYPE)


def validate_clear_queue_arguments(queue_type, queue_id):
    if queue_id is None or not is_valid_identifier(queue_id):
        raise BadArgumentException(INVALID_QUEUE_ID)

    if queue_type is None or not is_valid_identifier(queue_type):
        raise BadArgumentException(INVALID_QUEUE_TYPE)


def validate_get_queue_length_arguments(queue_type, queue_id):
    _validate_identifier(queue_type, INVALID_QUEUE_TYPE)
    _validate_identifier(queue_id, INVALID_QUEUE_ID)


def _validate_identifier(identifier, message):
    if not is_valid_identifier(identifier):
        raise BadArgumentException(message)
