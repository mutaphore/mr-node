"use strict";

const fs = require('fs');
const grpc = require('grpc');
const config = require('config');
const async = require('async');
const split = require('split');

const rpcFunc = require("./masterrpc");
const mr = require("../lib/mapreduce");
const utils = require("../lib/utils");

const STATE = mr.STATE;
const OP = mr.OP;

class Master {

  constructor(masterAddr, nMap, nReduce, fileName) {
    // general master configs
    this.masterAddr = masterAddr;
    this.nMap = nMap;
    this.nReduce = nReduce;
    this.fileName = fileName;
    this.mapJobsDone = [];  // number of map jobs completed
    this.reduceJobsDone = [];  // number of reduce jobs completed
    this.fileSplits = [];  // mapper file splits by byte number
    this.mapJobCount = 0;
    this.reduceJobCount = 0;
    this.heartbeatInterval = 5000;
    this.state = STATE.MAP;

    // worker directory: workerId -> worker object
    this.workers = {};

    // queues
    this.workerQueue = async.queue(this._dispatch.bind(this), 1);

    // create master rpc server
    this.server = new grpc.Server();
    this.server.bind(masterAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.masterDescriptor = grpc.load(config.get("proto.master")).masterrpc;
    this.workerDescriptor = grpc.load(config.get("proto.worker")).workerrpc;
    this.server.addProtoService(this.masterDescriptor.Master.service, {
      ping: rpcFunc.ping.bind(this),
      register: rpcFunc.register.bind(this),
      jobDone: rpcFunc.jobDone.bind(this),
      getMapSplit: rpcFunc.getMapSplit.bind(this),
      getWorkerInfo: rpcFunc.getWorkerInfo.bind(this),
    });
  }

  _nextState() {
    switch (this.state) {
      case STATE.MAP:
        this.state = STATE.WAIT_MAP;
        break;
      case STATE.WAIT_MAP :
        this.state = STATE.REDUCE;
        break;
      case STATE.REDUCE:
        this.state = STATE.WAIT_RED;
        break;
      case STATE.WAIT_RED:
        this.state = STATE.MERGE;
        break;
      case STATE.MERGE:
        this.state = STATE.END;
        break;
      default:
        // shouldn't get here
        console.log("Invalid master state");
        process.exit(1);
    }
  }

  _dispatch(worker, callback) {
    // dispatch actions according to current map reduce state
    switch (this.state) {
      case STATE.MAP:
        this._sendJob(OP.MAP, worker, callback);
        break;
      case STATE.WAIT_MAP:
        this._waitForJobs(OP.MAP, worker, callback);
        break;
      case STATE.REDUCE:
        this._sendJob(OP.REDUCE, worker, callback);
        break;
      case STATE.WAIT_RED:
        this._waitForJobs(OP.REDUCE, worker, callback);
        break;
      case STATE.MERGE:
        this._merge(callback);
        break;
      default:
        // shouldn't get here
        console.log("Invalid master state");
        return callback();
    }
  }

  _sendJob(operation, worker, callback) {
    // decide on the job number for this operation
    const jobNum = operation === OP.MAP ? this.mapJobCount : this.reduceJobCount;
    const n = operation === OP.MAP ? this.nMap : this.nReduce;
    if (jobNum >= n) {
      // TODO: look for straggler jobs not yet done and send workers to work on it
      // go to next state
      this._nextState();
      // push this worker back to the front of queue
      this.workerQueue.unshift(worker);
      return callback(null);
    }
    // send job request
    const data = { 
      job_number: jobNum, 
      operation: operation,
      file_name: this.fileName
    };
    worker.rpc.doJob(data, (err, resp) => {
      if (err) {
        return callback(err);
      }
      if (!resp || !resp.ok) {
        return callback(new Error("Invalid response received from worker"));
      }
      if (operation === OP.MAP) {
        this.mapJobCount = jobNum + 1;
      } else {
        this.reduceJobCount = jobNum + 1;
      }
      return callback(null);
    });
  }

  _waitForJobs(operation, worker, callback) {
    async.until(
      () => {
        return operation === OP.MAP ? 
          this.mapJobsDone.length === this.nMap : 
          this.reduceJobsDone.length === this.nReduce;
      },
      (callback) => {
        console.log("wait for jobs to complete");
        // check for job done every 1 second
        setTimeout(callback, 1000);
      },
      () => {
        // go to next state
        this._nextState();
        // push this worker back to front of the queue
        this.workerQueue.unshift(worker);
        return callback();
      }
    );
  }

  _merge(callback) {
    console.log("Merging result files");
    const tasks = [];
    const kvs = {};
    this.reduceJobsDone.forEach((reduceJob) => {
      tasks.push((callback) => {
        const reducerAddr = this.workers[reduceJob.workerId].address;
        const reducer = new this.workerDescriptor.Worker(reducerAddr, grpc.credentials.createInsecure());
        const data = {
          reducer_number: reduceJob.jobNum,
          file_name: this.fileName,
        };
        const rpcStream = reducer.getReducerOutput(data);
        rpcStream.on('data', (chunk) => {
          if (!chunk.line) {
            return;
          }
          const kv = JSON.parse(chunk.line);
          const key = Object.keys(kv)[0];
          kvs[key] = kv[key];
        })
        .on('end', () => {
          callback();
        })
        .on('error', (err) => {
          callback(err);
        });
      });
    });
    async.parallel(tasks, (err) => {
      // TODO: handle err properly
      const writeStream = fs.createWriteStream(`${this.fileName}-output`);
      // sort keys
      const sortedKeys = Object.keys(kvs).sort();
      sortedKeys.forEach((key) => {
        const kv = {};
        kv[key] = kvs[key];
        writeStream.write(JSON.stringify(kv) + '\n');
      });
      writeStream.end();
      writeStream.on('finish', () => {
        console.log('Done writing to output file');
        // go to next state
        this._nextState();
        this.workerQueue.kill();
        return callback();
      });
    });
  }

  _cleanup(callback) {
    // TODO...
    console.log("Stopping master..");
    process.exit(0);
  }

  start(callback) {
    utils.splitFileByLines(this.fileName, this.nMap, (err, fileSplits) => {
      if (err) {
        return callback(err);
      }
      this.fileSplits = fileSplits;
      // run the server
      this.server.start();
      console.log("Master running..");
      return callback(null, this);
    });
  }
}

exports.Master = Master;
