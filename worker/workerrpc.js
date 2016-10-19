"use strict";

function ping(call, callback) {
  const reply = { 
    host: this.workerAddr
  };
  return callback(null, reply);
}

function jobDone(call, callback) {

}

module.exports = {
  ping: ping,
  jobDone: jobDone
};