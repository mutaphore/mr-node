"use strict";

function ping(call, callback) {
  const reply = { 
    host: this.masterAddr
  };
  return callback(null, reply);
}

function register(call, callback) {

}

module.exports = {
  ping: ping,
  register: register
};