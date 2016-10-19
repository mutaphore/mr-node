"use strict";

function ping(call, callback) {
  const reply = { 
    host: this.workerAddr
  };
  return callback(null, reply);
}

function doJob(call, callback) {

}

module.exports = {
  ping: ping,
  doJob: doJob
};