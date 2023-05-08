import threading
from typing import Callable, Optional, Union
from amqpstorm.base import Stateful
from amqpstorm import Connection, Channel, AMQPError

OnErrorCallback = Union[
    None, Callable[[Exception], None]
]


class AMQPConnection:
    def __init__(
            self,
            host: str,
            username: str,
            password: str,
            port: int = 5672,
            heartbeat: int = 60,
            virtual_host: str = "/",
            on_error: OnErrorCallback = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.heartbeat = heartbeat
        self._on_error = on_error
        self._connection_lock = threading.Lock()
        self.channel: Optional[Channel] = None
        self._protocol: Optional[Connection] = None

    @property
    def connection_parameters(self):
        return {
            "hostname": self.host,

            "port": self.port,
            "username": self.username,
            "password": self.password,
            "virtual_host": self.virtual_host,
            # "on_error": self._on_error,
            "heartbeat": self.heartbeat,
        }

    @property
    def is_connected(self) -> bool:
        return self._protocol and self._protocol.current_state == Stateful.OPEN

    def has_channel_ready(self):
        return self.channel and self.channel.is_open

    def close(self) -> None:
        if not self.is_connected:
            return None
        self._protocol.close()

    def _connect(self) -> None:
        with self._connection_lock:
            if self.is_connected and self.has_channel_ready():
                return

            try:
                if self._protocol:
                    self.channel = self._protocol.channel()
                    return
            except AMQPError as e:
                # Se não conseguirmos pegar um channel novo
                # a conexão atual deve mesmo ser renovada e isso
                # será feito logo abaixo.
                pass

            self._protocol = Connection(**self.connection_parameters)

            self.channel = self._protocol.channel()
