"use strict";

const mr = require("../lib/mapreduce");

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
  callback(null, { ok: true });
  if (operation === mr.OP.MAP) {
    // this._doMapByFileName(jobNum, fileName);
    this._doMapByRpcStream(jobNum, fileName);
  } else {
    this._doReduce(jobNum, fileName);
  }
}

function getInterKeyValues(call, callback) {
  const mapJobNum = call.request.mapper_number;
  const reduceJobNum = call.request.reducer_number;
  // check if intermediate local file exists
  
}

module.exports = {
  ping,
  doJob,
  getInterKeyValues,
};