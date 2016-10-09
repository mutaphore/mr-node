"use strict";

function ping(call, callback) {
  const reply = { 
    host: this.workerAddr
  };
  return callback(null, reply);
}

module.exports = {
  ping: ping
};