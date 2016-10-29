"use strict";

const grpc    = require("grpc");
const config  = require("config");
const async   = require("async");
const rpcFunc = require("./masterrpc");
const mr      = require("../lib/mapreduce");

const STATE = mr.STATE;
const OP    = mr.OP;

class Master {

  constructor(masterAddr, nMap, nReduce) {
    // general master configs
    this.masterAddr = masterAddr;
    this.nMap = nMap;
    this.nReduce = nReduce;
    this.mapJobsDone = [];
    this.reduceJobsDone = [];
    this.mapJobCount = 0;
    this.reduceJobCount = 0;
    this.heartbeatInterval = 5000;
    this.state = STATE.MAP;

    // worker directory: workerId -> worker object
    this.workers = {};

    // queues
    this.workerQueue = async.queue(this._dispatch, 1);

    // create master rpc server
    this.server = new grpc.Server();
    this.server.bind(masterAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.masterDescriptor = grpc.load(config.get("proto.master")).masterrpc;
    this.workerDescriptor = grpc.load(config.get("proto.worker")).workerrpc;
    this.server.addProtoService(this.masterDescriptor.Master.service, {
      ping    : rpcFunc.ping.bind(this),
      register: rpcFunc.register.bind(this),
      jobDone : rpcFunc.jobDone.bind(this)
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
      case STATE.MERGE:
        this.state = STATE.END;
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
        return callback();
      default:
        // shouldn't get here
        console.log("Invalid master state");
        return callback();
    }
  }

  _sendJob(operation, worker, callback) {
    // decide on the job number for this operation
    let jobNum = (operation === OP.MAP ? this.mapJobCount : this.reduceJobCount) + 1;
    let n = operation === OP.MAP ? this.nMap : this.nReduce;
    if (jobNum > n) {
      // TODO: look for straggler jobs not yet done and send workers to work on it
      // go to next state
      this.nextState();
      // push this worker back to the front of queue
      this.workerQueue.unshift(worker);
      return callback(null);
    }
    // send job request
    worker.rpc.doJob({ job_num: jobNum, operation: operation }, (err, resp) => {
      if (err) {
        return callback(err);
      }
      if (!resp || !resp.ok) {
        return callback(new Error("Invalid response received from worker"));
      }
      if (operation === OP.MAP) {
        this.mapJobCount = jobNum;
      } else {
        this.reduceJobCount = jobNum;
      }
      return callback(null);
    });
  }

  _waitForJobs(operation, worker, callback) {
    async.until(
      () => {
        return operation === OP.MAP ? 
          this.mapJobsDone === this.nMap : 
          this.reduceJobsDone === this.nReduce;
      },
      (callback) => {
        // check for job done every 1 second
        setTimeout(callback, 1000);
      },
      () => {
        // go to next state
        this.nextState();
        // push this worker back to front of the queue
        this.workerQueue.unshift(worker);
        return callback();
      }
    );
  }

  _merge(callback) {
        
  }

  start() {
    // run the server
    this.server.start();
  }
}

exports.Master = Master;
