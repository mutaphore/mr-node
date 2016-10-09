"use strict";

function ping(call, callback) {
  const reply = { 
    host: this.masterAddr
  };
  return callback(null, reply);
}

function register(call, callback) {
  // create worker object
  const workerId   = call.worker_id;
  const workerAddr = call.worker_address;
  const worker     = {
    address: workerAddr,
    rpc: new this.workerDescriptor.Worker(workerAddr, grpc.credentials.createInsecure())
  };
  this.workers[workerId] = worker;

  // start heartbeat
  let firstBeat = true;
  setInterval(() => {
    worker.rpc.ping({ host: this.masterAddr }, (err, resp) => {
      if (err && firstBeat) {
        return callback(null, false);
      } else if (err && !firstBeat) {
        // remove worker from list
        delete this.workers[workerId];
      }
      if (firstBeat) {
        firstBeat = false;
        return callback(null, true)
      }
      console.log(`Connected to worker ${workerId} at: ${resp.host}`);
    });
  }, this.heartbeatInterval);
}

module.exports = {
  ping: ping,
  register: register
};