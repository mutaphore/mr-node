"use strict";

// get worker host info
function ping(call, callback) {
  const reply = { 
    host: this.workerAddr
  };
  return callback(null, reply);
}

module.exports = {
  ping: ping
};