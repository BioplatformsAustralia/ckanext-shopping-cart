ckan.module("shopping-cart-clear", function ($) {
  const { CART_REFRESH } = CkanextShoppingCart.events;
  return {
    options: {
      scope: null,
      cart: null,
      username: null
    },
    initialize: function () {
      this.el.on("click", () => this.clear());
    },

    clear: function () {
      const { scope, cart, username } = this.options;
      const payload = { cart, scope, username };

      this.sandbox.client.call(
        "POST",
        "shopping_cart_clear",
        payload,
        (data) => this._onClearSucceed(data),
        (resp) => this._onClearFailed(resp)
      );
    },

    _onClearSucceed: function (data) {
      this.sandbox.publish(CART_REFRESH, this.options.cart, data.result);
    },

    _onClearFailed: function (resp) {
      console.warn(resp);
    },
  };
});
