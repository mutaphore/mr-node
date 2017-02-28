"use strict";

const fs = require('fs');
const split = require('split');
const mr = require('../lib/mapreduce');

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
  // return OK immediately
  callback(null, { ok: true });
  if (operation === mr.OP.MAP) {
    // this._doMapByFileName(jobNum, fileName);
    this._doMapByRpcStream(jobNum, fileName);
  } else {
    // this._doReduceByFileName(jobNum, fileName);
    this._doReduceByRpcStream(jobNum, fileName);
  }
}

function getInterKeyValues(call) {
  const mapJobNum = call.request.mapper_number;
  const reduceJobNum = call.request.reducer_number;
  const fileName = call.request.file_name;
  // check if intermediate local file exists
  const path = mr.reduceFileName(fileName, mapJobNum, reduceJobNum);
  if (!fs.existsSync(path)) {
    return callback(new Error(`File '${path}' does not exist`));
  }
  fs.createReadStream(path)
    .pipe(split())
    .on('data', (line) => {
      call.write({ key_value: line });
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

module.exports = {
  ping,
  doJob,
  getInterKeyValues,
};