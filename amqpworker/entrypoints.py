from abc import ABC
from asyncio import iscoroutinefunction
from typing import Generic, TypeVar

import amqpworker
from amqpworker.routes import RouteHandler

T = TypeVar("T")


def _extract_sync_callable(handler) -> RouteHandler:
    cb = handler
    if not callable(cb):
        raise TypeError(f"Object passed as handler is not callable: {cb}")
    if iscoroutinefunction(cb) or (hasattr(cb, '__call__') and iscoroutinefunction(cb)):
        raise TypeError(f"handler must be sync: {cb}")
    return cb


class EntrypointInterface(ABC, Generic[T]):
    def __init__(self, app: "amqpworker.App") -> None:
        self.app = app
