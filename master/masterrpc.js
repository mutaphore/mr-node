"use strict";

const grpc = require("grpc");

function ping(call, callback) {
  const reply = { 
    host: this.masterAddr
  };
  return callback(null, reply);
}

function register(call, callback) {
  // create worker object
  const workerId   = call.request.worker_id;
  const workerAddr = call.request.worker_address;
  const worker     = {
    address: workerAddr,
    rpc: new this.workerDescriptor.Worker(workerAddr, grpc.credentials.createInsecure())
  };
  this.workers[workerId] = worker;

  // start heartbeat
  let firstBeat = true;
  const interval = setInterval(() => {
    worker.rpc.ping({ host: this.masterAddr }, (err, resp) => {
      if (err && firstBeat) {
        // error on first heartbeat
        console.error("Failed to connect with worker");
        return callback(null, { success: false });
      } else if (err && !firstBeat) {
        // remove worker from list and stop pinging
        console.error("Failed to connect with worker");
        delete this.workers[workerId];
        return clearInterval(interval);
      } else if (!resp) {
        // no response
        console.error("No response from worker, closing connection");
        delete this.workers[workerId];
        return clearInterval(interval);
      }
      // return success response if this is the first ping
      if (firstBeat) {
        firstBeat = false;
        return callback(null, { success: true });
      }
      console.log(`Pinging worker ${workerId} at ${workerAddr}`);
    });
  }, this.heartbeatInterval);
}

module.exports = {
  ping: ping,
  register: register
};