"use strict";

const grpc    = require("grpc");
const config  = require("config");
const async   = require("async");
const rpcFunc = require("./masterrpc");

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
    this.state = 'map';   // map -> waitForMap -> reduce -> waitForReduce

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

  _dispatch(worker, callback) {
    // dispatch actions according to current map reduce state
    switch (this.state) {
      case 'map':
        this._sendJob('map', worker, callback);
        break;
      case 'waitForMap':
        this._waitForMap(callback);
        break;
      case 'reduce':
        this._sendJob('reduce', worker, callback);
        break;
      case 'waitForReduce':
        this._waitForReduce(callback);
        break;
      default:
        // shouldn't get here
        console.log("Invalid master state");
        return callback();
    }
  }

  _sendJob(operation, worker, callback) {
    // decide on the job number for this operation
    let jobNum = operation === 'map' ? this.mapJobCount : this.reduceJobCount;
    jobNum++;
    // send job request
    worker.rpc.doJob({ job_num: jobNum, operation: operation }, (err, resp) => {
      if (err) {
        return callback(err);
      }
      if (!resp || !resp.ok) {
        return callback(new Error("Invalid response received from worker"));
      }
      if (operation === 'map') {
        this.mapJobCount = jobNum;
      } else {
        this.reduceJobCount = jobNum;
      }
      return callback(null);
    });
  }

  _waitForMap(callback) {

  }

  _waitForReduce(callback) {

  }

  start() {
    // run the server
    this.server.start();
  }
}

exports.Master = Master;
