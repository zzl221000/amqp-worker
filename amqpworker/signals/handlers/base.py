from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from amqpworker import App  # noqa: F401


class SignalHandler:
    def startup(self, app: "App"):
        pass  # pragma: no cover

    def shutdown(self, app: "App"):
        pass  # pragma: no cover