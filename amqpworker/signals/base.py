import abc
import threading

from collections import UserList


class Freezable(metaclass=abc.ABCMeta):
    def __init__(self):
        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    def freeze(self):
        self._frozen = True


class Signal(UserList, threading.Event):
    def __init__(self, owner: Freezable):
        UserList.__init__(self)
        threading.Event.__init__(self)
        self._owner = owner
        self.frozen = False

    def __repr__(self):
        return "<Signal owner={}, frozen={}, {!r}>".format(
            self._owner, self.frozen, list(self)
        )

    def send(self, *args, **kwargs):
        """
        Sends data to all registered receivers.
        """
        if self.frozen:
            raise RuntimeError("Cannot send on frozen signal.")

        for receiver in self:
            receiver(*args, **kwargs)

        self.frozen = True
        self._owner.freeze()
        self.set()
