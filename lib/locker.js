"use strict";

const lockingCache = require("locking-cache");

function close(val) {
  val[0] && val[0].close && val[0].close();
}

const locker = lockingCache({
  max: 1000,
  dispose: async (key, val) => close(val)
});

process.on("exit", code => {
  lockedLoad.cache.forEach(v => close(v));
});

module.exports = locker;