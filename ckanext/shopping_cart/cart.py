from __future__ import annotations

import abc
import pickle
import dataclasses
from typing import Any, Iterable, Optional, OrderedDict

import ckan.lib.redis as redis
import ckan.plugins.toolkit as tk
from ckan.common import session
from ckan.common import g
import ckan.lib.helpers as h
import json
import ckan.logic as logic

CONFIG_CART_FACTORY = "ckanext.shopping_cart.factory.default"
DEFAULT_CART_FACTORY = "session"


def get_cart(scope: str, context: dict[str, Any], data_dict):
    factory = factories[
        tk.config.get(CONFIG_CART_FACTORY, DEFAULT_CART_FACTORY)
    ]
    cart = factory()

    cart.data_dict = data_dict
    cart.identify(scope, context)
    return cart


@dataclasses.dataclass
class Item:
    id: str
    details: dict[str, Any] = dataclasses.field(default_factory=dict)


class Cart(abc.ABC):
    id: Optional[str] = None
    content: Any
    data_dict: Any

    def __init__(self):
        self.clear()

    def clear(self):
        self.content = {}

    def identify(self, scope: str, context: dict[str, Any]):
        id_: str = context["user"]
        if scope == "session":
            id_ = getattr(session, "id", "")

        self.id = id_

    def add(self, item: Any, details: dict[str, Any]):
        self.content[item] = details

    def pop(self, item: Any):
        return self.content.pop(item, None)

    def show(self):
        return [
            {"id": id, "details": details}
            for id, details in self.content.items()
        ]

    def __bool__(self):
        return bool(self.content)

    @abc.abstractmethod
    def restore(self, key: str):
        pass

    @abc.abstractmethod
    def save(self, key: str):
        pass

    @abc.abstractmethod
    def drop(self, key: str):
        pass


class RedisCart(Cart):
    def __init__(self):
        super().__init__()
        self.conn = redis.connect_to_redis()
        site_id = tk.config["ckan.site_id"]
        self.prefix = f"ckan:{site_id}:ckanext:shopping_cart:{self.id}"

    def restore(self, key: str):
        data = self.conn.get(self.prefix + key)
        if not data:
            self.clear()
            return
        self.content = pickle.loads(data)

    def save(self, key: str):
        self.conn.set(self.prefix + key, pickle.dumps(self.content))

    def drop(self, key: str):
        self.conn.delete(self.prefix + key)


class SessionCart(Cart):
    def __init__(self):
        super().__init__()
        self.session = session

    def restore(self, key: str):
        data = self.session.get(f"shopping_cart:{key}")
        if not data:
            self.clear()
            return
        self.content = data

    def save(self, key: str):
        self.session[f"shopping_cart:{key}"] = self.content

    def drop(self, key: str):
        self.session.pop(f"shopping_cart:{key}", None)


class UserCart(Cart):
    def __init__(self):
        super().__init__()
        self.site_user = tk.get_action("get_site_user")({'ignore_auth': True}, {})["name"]
        self.admin_ctx = {"ignore_auth": True, "user": self.site_user }

    def identify(self, scope: str, context: dict[str, Any]):
        self.scope = scope
        self.username = self.data_dict['__extras']['username']
        self.id = self.username

    def get_user(self):
        # Default to own user
        username = g.userobj.name
        # Only admins can view another user's cart
        if g.userobj.name != self.username:
            if g.userobj.sysadmin is True:
                username = self.username

        user_id = { "id": username, "include_plugin_extras": True }
        user = tk.get_action('user_show')(self.admin_ctx, user_id)
        return user

    def restore(self, key: str):
        user = self.get_user()
        plugin_extras = self.get_plugin_extras(user)
        self.content = self.get_cart(plugin_extras, key)

    def save(self, key: str):
        user = self.get_user()
        plugin_extras = self.get_plugin_extras(user)
        plugin_extras[self.get_cart_key(key)] = self.content
        user['plugin_extras'] = plugin_extras
        tk.get_action('user_update')(self.admin_ctx, user)

    def drop(self, key: str):
        self.clear()
        self.save(key)
    
    def get_plugin_extras(self, user):
        if 'plugin_extras' in user and user.get('plugin_extras') is not None:
            return user.get('plugin_extras')
        else:
            return {}
    
    def get_cart(self, plugin_extras, cart_name):
        cart_key = self.get_cart_key(cart_name)
        if plugin_extras is not None and cart_key in plugin_extras: 
            return plugin_extras[cart_key]
        else:
            return {}      

    def get_cart_key(self, key):
        return f"shopping_cart__{key}"

class FakeSessionCart(SessionCart):
    def __init__(self):
        super().__init__()

        if "shopping_cart_session" not in tk.g:
            tk.g.shopping_cart_session = {}
        self.session = tk.g.shopping_cart_session


factories = {
    "redis": RedisCart,
    "session": SessionCart,
    "user_plugin_extras": UserCart,
}
