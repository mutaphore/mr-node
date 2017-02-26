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
    this._doMapByStream(jobNum, fileName);
  } else {
    this._doReduce(jobNum, fileName);
  }
}

module.exports = {
  ping: ping,
  doJob: doJob
};