"use strict";

const grpc = require('grpc');
const fs = require('fs');
const split = require('split');
const mr = require('../lib/mapreduce');

const STATE = mr.STATE;
const OP = mr.OP;

function ping(call, callback) {
  const reply = { 
    host: this.masterAddr
  };
  return callback(null, reply);
}

function register(call, callback) {
  // create worker object
  const workerId = call.request.worker_id;
  const workerAddr = call.request.worker_address;
  const worker = {
    id: workerId,
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
        return callback(null, { ok: false });
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
        // add worker to queue on first ping
        this.workerQueue.push(worker);
        return callback(null, { 
          ok: true,
          n_map: this.nMap,
          n_reduce: this.nReduce,
        });
      }
      this.log.info(`Pinging worker ${workerId} at ${workerAddr}`);
    });
  }, this.heartbeatInterval);
}

function jobDone(call, callback) {
  const worker = this.workers[call.request.worker_id];
  // record job done
  if (!call.request.error) {
    if (call.request.operation === OP.MAP) {
      this.mapJobsDone.push({
        workerId: call.request.worker_id,
        jobNum: call.request.job_number,
      });
    } else if (call.request.operation === OP.REDUCE) {
      this.reduceJobsDone.push({
        workerId: call.request.worker_id,
        jobNum: call.request.job_number,
      });
    }
  } else {
    this.log.error(`Worker job error: ${call.request.error}`);
  }
  // put worker back into queue
  if (worker) {
    this.workerQueue.push(worker);
  }
  return callback(null, { ok: true });
}

function getMapSplit(call) {
  const worker = this.workers[call.request.worker_id];
  const jobNum = call.request.job_number;
  // check if we're still doing Map, if not return error
  if (this.state !== STATE.MAP) {
    console.error('Not in Map state');
    call.end();
    // TODO: handle error gracefully
    this.workerQueue.push(worker)
    return;
  }
  fs.createReadStream(this.fileName, this.fileSplits[jobNum])
    .pipe(split())  // split file by lines
    .on('data', (line) => {
      call.write({ line });
    })
    .on('end', () => {
      call.end();
    })
    .on('error', (err) => {
      // TODO: handle error gracefully
      console.error(err);
      call.end();
    });
}

function getWorkerInfo(call, callback) {
  let table = {};
  const mapperAddrs = [];
  const reducerAddrs = [];
  this.mapJobsDone.forEach((job) => {
    table[job.jobNum] = this.workers[job.workerId].address;
  });
  for (let i = 0; i < this.nMap; i++) {
    if (table[i]) {
      mapperAddrs.push(table[i]);
    } else {
      mapperAddrs.push('');
    }
  }
  table = {};
  this.reduceJobsDone.forEach((job) => {
    table[job.jobNum] = this.workers[job.workerId].address;
  });
  for (let i = 0; i < this.nReduce; i++) {
    if (table[i]) {
      reducerAddrs.push(table[i]);
    } else {
      reducerAddrs.push('');
    }
  }
  return callback(null, {
    mapper_addresses: mapperAddrs,
    reducer_addresses: reducerAddrs,
  });
}

module.exports = {
  ping,
  register,
  jobDone,
  getMapSplit,
  getWorkerInfo,
};