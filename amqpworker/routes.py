import abc
from collections import UserDict
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    Union,
)

from cached_property import cached_property
from pydantic import BaseModel, Extra, root_validator, validator

from amqpworker import conf
from amqpworker.connections import AMQPConnection, Connection
from amqpworker.options import Actions, DefaultValues, RouteTypes

RouteHandler = Callable[..., Coroutine]


class Model(BaseModel, abc.ABC):
    """
    An abstract pydantic BaseModel that also behaves like a Mapping
    """

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError as e:
            raise KeyError from e

    def __setitem__(self, key, value):
        try:
            return self.__setattr__(key, value)
        except (AttributeError, ValueError) as e:
            raise KeyError from e

    def __eq__(self, other):
        if isinstance(other, dict):
            return self.dict() == other
        return super(Model, self).__eq__(other)

    def __len__(self):
        return len(self.__fields__)

    def keys(self):
        return self.__fields__.keys()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


class _RouteOptions(Model):
    pass


class Route(Model, abc.ABC):
    """
    An abstract Model that acts like a route factory
    """

    type: RouteTypes
    handler: Any
    routes: List[str]
    connection: Optional[Connection]
    options: _RouteOptions = _RouteOptions()

    @staticmethod
    def factory(data: Dict) -> "Route":
        try:
            type_ = data.pop("type")
        except KeyError as e:
            raise ValueError("Routes must have a type") from e

        if type_ == RouteTypes.HTTP:
            raise ValueError(f"'{type_}' is an invalid RouteType.")
        if type_ == RouteTypes.AMQP_RABBITMQ:
            return AMQPRoute(**data)
        raise ValueError(f"'{type_}' is an invalid RouteType.")


class AMQPRouteOptions(_RouteOptions):
    bulk_size: int = DefaultValues.BULK_SIZE
    max_workers: int = DefaultValues.MAX_SUBMIT_WORKER_SIZE
    bulk_flush_interval: int = DefaultValues.BULK_FLUSH_INTERVAL
    on_success: Actions = DefaultValues.ON_SUCCESS
    on_exception: Actions = DefaultValues.ON_EXCEPTION
    connection_fail_callback: Optional[
        Callable[[Exception, int], Coroutine]
    ] = None
    connection: Optional[Union[AMQPConnection, str]]

    class Config:
        arbitrary_types_allowed = False
        extra = Extra.forbid


class AMQPRoute(Route):
    type: RouteTypes = RouteTypes.AMQP_RABBITMQ
    vhost: str = conf.settings.AMQP_DEFAULT_VHOST
    connection: Optional[AMQPConnection]
    options: AMQPRouteOptions


class RoutesRegistry(UserDict):
    def _get_routes_for_type(self, route_type: Type) -> Iterable:
        return tuple((r for r in self.values() if isinstance(r, route_type)))

    @cached_property
    def amqp_routes(self) -> Iterable[AMQPRoute]:
        return self._get_routes_for_type(AMQPRoute)

    def __setitem__(self, key: RouteHandler, value: Union[Dict, Route]):
        if not isinstance(value, Route):
            route = Route.factory({"handler": key, **value})
        else:
            route = value

        super(RoutesRegistry, self).__setitem__(key, route)

    def add_route(self, route: Route) -> None:
        self[route.handler] = route

    def add_amqp_route(self, route: AMQPRoute) -> None:
        self[route.handler] = route

    def route_for(self, handler: RouteHandler) -> Route:
        return self[handler]
