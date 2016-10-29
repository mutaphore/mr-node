"use strict";

const reader = require("../lib/reader");
const mr = require("../lib/mapreduce");

const OP = mr.OP;

function ping(call, callback) {
  const reply = { 
    host: this.workerAddr
  };
  return callback(null, reply);
}

function doJob(call, callback) {
  const operation = call.request.operation;
  const jobNum = call.request.job_number;
  const fileName = call.request.file_name;
  // immediately return ok to master asynchronously
  callback(null, { ok: true });
  if (operation === OP.MAP) {
    this._doMap(jobNum, fileName);
  } else {
    this._doReduce(jobNum, fileName);
  }
}

module.exports = {
  ping: ping,
  doJob: doJob
};